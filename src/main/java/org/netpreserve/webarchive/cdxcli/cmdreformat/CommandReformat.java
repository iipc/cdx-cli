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
package org.netpreserve.webarchive.cdxcli.cmdreformat;

import java.io.BufferedWriter;
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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.netpreserve.commons.cdx.CdxFormat;
import org.netpreserve.commons.cdx.CdxRecord;
import org.netpreserve.commons.cdx.CdxSource;
import org.netpreserve.commons.cdx.cdxrecord.CdxjLineFormat;
import org.netpreserve.commons.cdx.SearchKey;
import org.netpreserve.commons.cdx.SearchResult;
import org.netpreserve.commons.cdx.cdxsource.BlockCdxSource;
import org.netpreserve.commons.cdx.cdxsource.CdxFileDescriptor;
import org.netpreserve.commons.cdx.cdxsource.CdxSourceExecutorService;
import org.netpreserve.commons.cdx.cdxsource.MultiCdxSource;
import org.netpreserve.commons.cdx.formatter.CdxRecordFormatter;
import org.netpreserve.commons.cdx.sort.SortingWriter;
import org.netpreserve.webarchive.cdxcli.Command;
import org.netpreserve.webarchive.cdxcli.MainParameters;

/**
 * Command for reformatting from one version of cdx to another.
 */
@Parameters(commandNames = "reformat", commandDescription = "Reformat cdx file")
public class CommandReformat implements Command {

    @Parameter(names = {"-f", "--format"}, description = "One of cdxj, cdx9 or cdx11.")
    CdxFormat format = CdxjLineFormat.DEFAULT_CDXJLINE;

    @Parameter(names = {"-s", "--sort"}, description = "Sort file after reformatting")
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

    @Override
    public void exec(MainParameters mp) {
        String outFileSuffix = "." + format.getFileSuffix();

        if (outputFileName == null) {
            // Wrtie to std out
            Writer dst = new OutputStreamWriter(System.out, StandardCharsets.UTF_8);
            try (Writer out = createOutput(dst);
                    CdxSource src = createMultiCdxSource(inputFileNames);) {
                reformat(src, out);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        } else {
            Path outPath = Paths.get(outputFileName);
            if (concatenate) {
                Path outFile;
                if (Files.isDirectory(outPath)) {
                    outFile = outPath.resolve("out" + outFileSuffix);
                } else {
                    outFile = outPath;
                }

                System.out.println("Reformatting: ");
                inputFileNames.stream().forEach((in) -> {
                    System.out.println("  " + in);
                });
                System.out.println("into: " + outFile);

                try (Writer out = createOutput(outFile);
                        CdxSource src = createMultiCdxSource(inputFileNames);) {
                    reformat(src, out);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            } else {
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

                    System.out.println("Reformatting: " + in + " into: " + outFile);

                    try (Writer out = createOutput(outFile);
                            CdxSource src = createCdxSource(in);) {
                        reformat(src, out);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }
            }
        }
        CdxSourceExecutorService.getInstance().shutdown();
    }

    /**
     * Create a writer from an output file.
     * <p>
     * @param outFile a file to send the result to
     * @return the newly created writer
     * @throws IOException is thrown if the output file already exists or the underlying IO classes throws an exception.
     */
    Writer createOutput(Path outFile) throws IOException {
        if (Files.exists(outFile)) {
            throw new UncheckedIOException(new IOException(outFile + " already exists"));
        }

        Writer out = new FileWriter(outFile.toFile());
        out = new BufferedWriter(out);

        out.write(format.getFileHeader());
        out.write('\n');

        if (sort) {
            out = new SortingWriter(out, scratchfileCount, heapSize);
        }

        return out;
    }

    /**
     * Create a writer from another writer.
     * <p>
     * @param out a writer to send the result to
     * @return the newly created writer
     * @throws IOException is thrown if the output file already exists or the underlying IO classes throws an exception.
     */
    Writer createOutput(Writer out) throws IOException {
        if (!(out instanceof BufferedWriter)) {
            out = new BufferedWriter(out);
        }

        out.write(format.getFileHeader());
        out.write('\n');

        if (sort) {
            out = new SortingWriter(out, scratchfileCount, heapSize);
        }

        return out;
    }

    /**
     * Do the reformatting and write the result to a {@link Writer}.
     * <p>
     * @param src the cdx input
     * @param out an {@link Writer} to send the result to
     * @throws IOException is thrown if the underlying IO classes could not read or write
     */
    void reformat(CdxSource src, Writer out) throws IOException {
        SearchResult result = src.search(new SearchKey(), null, false);

        CdxRecordFormatter formatter = new CdxRecordFormatter(format);

        for (CdxRecord cdxLine : result) {
            formatter.format(out, cdxLine, true);
            out.append('\n');
        }

        out.flush();
    }

    /**
     * Create a {@link CdxSource} from a file name.
     * <p>
     * @param fileName the file name of the cdx file
     * @return a CdxSource representing the input file
     */
    private CdxSource createCdxSource(String fileName) {
        try {
            return new BlockCdxSource(new CdxFileDescriptor(Paths.get(fileName), false));
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Create a {@link CdxSource} from a set of file names.
     * <p>
     * @param fileNames the file names of the cdx files
     * @return a CdxSource representing the input files
     */
    private CdxSource createMultiCdxSource(List<String> fileNames) {
        try {
            MultiCdxSource src = new MultiCdxSource();
            for (String fileName : fileNames) {
                src.addSource(new BlockCdxSource(new CdxFileDescriptor(Paths.get(fileName), false)));
            }

            return src;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

}
