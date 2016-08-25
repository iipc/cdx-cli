/*
 * Copyright 2016 The International Internet Preservation Consortium.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.netpreserve.webarchive.cdxcli.cmdextract;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.jwat.archive.FileIdent;
import org.netpreserve.commons.cdx.CdxFormat;
import org.netpreserve.commons.cdx.cdxrecord.CdxjLineFormat;
import org.netpreserve.commons.cdx.formatter.CdxRecordFormatter;
import org.netpreserve.webarchive.cdxcli.Command;
import org.netpreserve.webarchive.cdxcli.MainParameters;

/**
 * Command for extracting cdx records from ARC and WARC files.
 */
@Parameters(commandNames = "extract", commandDescription = "Extract cdx file from ARC/WARC files")
public class CommandExtract implements Command {

    @Parameter(names = {"-f", "--format"}, description = "One of cdxj, cdx9 or cdx11.")
    CdxFormat format = CdxjLineFormat.DEFAULT_CDXJLINE;

    @Parameter(names = {"-s", "--sort"}, description = "Sort file after extracting")
    boolean sort = false;

    @Parameter(names = {"-c", "--concatenate"}, description = "Concatenate output into one file")
    boolean concatenate = false;

    @Parameter(names = {"-i", "--input"}, required = true, variableArity = true, description = "Input file. "
               + "Multiple values can be separated by comma, space or the parameter can be repeated. "
               + "Separation by space means that shell expansion will work.")
    List<String> inputFileNames;

    @Parameter(names = {"-o", "--output"}, description = "Destination. If not given, standard out is used. "
               + "If -c is given, the output will be treated as a file name. "
               + "If output is a directory, result is written to '<output>/out.<suffix>'. "
               + "If -c is not given, output must be a directory and the filenames will be equal to the input "
               + "except for the suffix.")
    String outputFileName;

    @Parameter(names = {"-t", "--tempfiles"}, description = "The number of temporary files used for sorting. "
               + "Only applicable when parameter -s is set")
    int scratchfileCount = 10;

    @Parameter(names = {"-h", "--heapsize"}, description = "The number of lines in the heap when sorting. "
               + "The amount of memory used is dependent on average cdx line length. "
               + "Only applicable when parameter -s is set")
    int heapSize = 100;

    final CdxExtractor cdxExtractor = new CdxExtractor();

    @Override
    public void exec(MainParameters mp) {
        String outFileSuffix = "." + format.getFileSuffix();
        CdxRecordFormatter formatter = new CdxRecordFormatter(format);

        if (outputFileName == null) {
            // Wrtie to std out
            Writer dst = new OutputStreamWriter(System.out, StandardCharsets.UTF_8);
            try (Output out = createOutput(dst, formatter);) {
                for (String in : inputFileNames) {
                    File inFile = new File(in);
                    extract(inFile, out);
                }
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        } else {
            Path outPath = Paths.get(outputFileName);
            if (concatenate) {
                // Write into single out file
                Path outFile;
                if (Files.isDirectory(outPath)) {
                    outFile = outPath.resolve("out" + outFileSuffix);
                } else {
                    outFile = outPath;
                }

                System.err.println("Extracting: ");
                inputFileNames.stream().forEach((in) -> {
                    System.out.println("  " + in);
                });
                System.err.println("Number of input files: " + inputFileNames.size());
                System.err.println("into: " + outFile);

                try (Output out = createOutput(outFile, formatter)) {
                    ExecutorService executor = Executors.newFixedThreadPool(Math.min(inputFileNames.size(), 16));
                    for (String in : inputFileNames) {
                        File inFile = new File(in);
                        executor.submit(new WarcReaderThread(inFile, out));
                    }
                    executor.shutdown();
                    executor.awaitTermination(5, TimeUnit.HOURS);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                // Write to one file per (w)arc file.
                if (!Files.isDirectory(outPath)) {
                    throw new IllegalArgumentException("Output " + outputFileName + " must be a directory");
                }
                int i = 0;
                for (String in : inputFileNames) {
                    String inName = Paths.get(in).getFileName().toString();
                    if (inName.contains(".")) {
                        inName = inName.substring(0, inName.lastIndexOf('.'));
                    }
                    inName += outFileSuffix;
                    Path outFile = outPath.resolve(inName);

                    System.err.println("Extracting: " + in + " into: " + outFile);

                    try (Output out = createOutput(outFile, formatter)) {
                        File inFile = new File(in);
                        extract(inFile, out);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }
            }
        }
    }

    private class WarcReaderThread implements Runnable {

        private final File inFile;

        private final Output out;

        public WarcReaderThread(File inFile, Output out) {
            this.inFile = inFile;
            this.out = out;
        }

        @Override
        public void run() {
            try {
                extract(inFile, out);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

    /**
     * Create a Output from an output file.
     * <p>
     * @param outFile a file to send the result to
     * @param formatter the formatter used to serialize the records
     * @return the newly created Output
     * @throws IOException is thrown if the output file already exists or the underlying IO classes throws an exception.
     */
    Output createOutput(Path outFile, CdxRecordFormatter formatter) throws IOException {
        if (Files.exists(outFile)) {
            throw new UncheckedIOException(new IOException(outFile + " already exists"));
        }

        Writer out = new FileWriter(outFile.toFile());
        BufferedWriter bufferedOut = new BufferedWriter(out);

        bufferedOut.write(format.getFileHeader());
        bufferedOut.write('\n');

        if (sort) {
            return new SortingOutput(bufferedOut, formatter, scratchfileCount, heapSize);
        } else {
            return new SerialOutput(bufferedOut, formatter);
        }
    }

    /**
     * Create a Output from a writer.
     * <p>
     * @param out a writer to send the result to
     * @param formatter the formatter used to serialize the records
     * @return the newly created Output
     * @throws IOException is thrown if the underlying IO classes throws an exception.
     */
    Output createOutput(Writer out, CdxRecordFormatter formatter) throws IOException {
        BufferedWriter bufferedOut;

        if (out instanceof BufferedWriter) {
            bufferedOut = (BufferedWriter) out;
        } else {
            bufferedOut = new BufferedWriter(out);
        }

        bufferedOut.write(format.getFileHeader());
        bufferedOut.write('\n');

        if (sort) {
            return new SortingOutput(bufferedOut, formatter, scratchfileCount, heapSize);
        } else {
            return new SerialOutput(bufferedOut, formatter);
        }
    }

    /**
     * Do the extraction and write the result to a {@link Writer}.
     * <p>
     * @param src the cdx input
     * @param out an {@link Output} to send the result to
     * @throws IOException is thrown if the underlying IO classes could not read or write
     */
    void extract(File src, Output out) throws IOException {
        FileIdent fileIdent = FileIdent.ident(src);
        if (src.length() > 0) {
            if (fileIdent.filenameId != fileIdent.streamId) {
                System.err.println("Extension not in line with content: '" + src.getPath() + "', processing anyway.");
            }
            switch (fileIdent.streamId) {
                case FileIdent.FILEID_ARC:
                case FileIdent.FILEID_ARC_GZ:
                case FileIdent.FILEID_WARC:
                case FileIdent.FILEID_WARC_GZ:
                    System.err.println("Processing file: '" + src.getPath() + "'");

                    cdxExtractor.process(src, fileIdent, out);
                    break;
                default:
                    System.err.println("Not a (W)ARC file: '" + src.getPath() + "'");
                    break;
            }
        } else {
            switch (fileIdent.filenameId) {
                case FileIdent.FILEID_ARC:
                case FileIdent.FILEID_ARC_GZ:
                case FileIdent.FILEID_WARC:
                case FileIdent.FILEID_WARC_GZ:
                    System.err.println("Empty file: '" + src.getPath() + "'");
                    break;
                default:
                    System.err.println("Not a (W)ARC file: '" + src.getPath() + "'");
                    break;
            }
        }
    }

}
