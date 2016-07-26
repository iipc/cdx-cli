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
import org.netpreserve.commons.cdx.CdxLineFormat;
import org.netpreserve.commons.cdx.CdxRecord;
import org.netpreserve.commons.cdx.CdxSource;
import org.netpreserve.commons.cdx.CdxjLineFormat;
import org.netpreserve.commons.cdx.SearchKey;
import org.netpreserve.commons.cdx.SearchResult;
import org.netpreserve.commons.cdx.cdxsource.BlockCdxSource;
import org.netpreserve.commons.cdx.cdxsource.CdxFileDescriptor;
import org.netpreserve.commons.cdx.cdxsource.CdxSourceExecutorService;
import org.netpreserve.commons.cdx.cdxsource.MultiCdxSource;
import org.netpreserve.commons.cdx.formatter.CdxRecordFormatter;
import org.netpreserve.webarchive.cdxcli.Command;
import org.netpreserve.webarchive.cdxcli.MainParameters;
import org.netpreserve.webarchive.cdxcli.ParameterNotSupported;

/**
 * Command for reformatting from one version of cdx to another.
 */
@Parameters(commandNames = "reformat", commandDescription = "Reformat cdx file")
public class CommandReformat implements Command {

    @Parameter(names = {"-f", "--format"}, description = "one of cdxj, cdx9 or cdx11.",
               validateWith = FormatValidator.class)
    String format = "cdxj";

    @Parameter(names = {"-s", "--sort"}, description = "sort file after reformatting",
               validateWith = ParameterNotSupported.class)
    boolean sort = false;

    @Parameter(names = {"-c", "--concatenate"}, description = "concatenate output into one file")
    boolean concatenate = false;

    @Parameter(names = {"-i", "--input"}, required = true, variableArity = true, description = "input file. "
               + "Multiple values can be separated by comma or the parameter can be repeated")
    List<String> inputFileNames;

    @Parameter(names = {"-o", "--output"}, description = "destination. If not given, standard out is used. "
            + "If -c is given, the output will be treated as a file name. "
            + "If output is a directory, result is written to <output>/out.<suffix>. "
            + "If -c is not given, output must be a directory and the filenames will be equal to the input "
            + "except for the suffix.")
    String outputFileName;

    @Override
    public void exec(MainParameters mp) {
        String outFileSuffix;
        switch (format) {
            case "cdxj":
                outFileSuffix = ".cdxj";
                break;
            case "cdx9":
            case "cdx11":
                outFileSuffix = ".cdx";
                break;
            default:
                outFileSuffix = null;
        }

        if (outputFileName == null) {
            try (CdxSource src = createMultiCdxSource(inputFileNames);
                    Writer dst = new OutputStreamWriter(System.out, StandardCharsets.UTF_8);) {
                reformat(src, dst);
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

                try (CdxSource src = createMultiCdxSource(inputFileNames);) {
                    reformat(src, outFile);
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

                    try (CdxSource src = createCdxSource(in);) {
                        reformat(src, outFile);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }
            }
        }
        CdxSourceExecutorService.getInstance().shutdown();
    }

    /**
     * Do the reformatting and write the result to a file.
     * <p>
     * @param src the cdx input
     * @param outFile a file to send the result to
     * @throws IOException is thrown if the output file already exists or the underlying IO classes throws an exception.
     */
    void reformat(CdxSource src, Path outFile) throws IOException {
        if (Files.exists(outFile)) {
            throw new UncheckedIOException(new IOException(outFile + " already exists"));
        }

        try (Writer out = new FileWriter(outFile.toFile());) {
            reformat(src, out);
        }
    }

    /**
     * Do the reformatting and write the result to a {@link Writer}.
     * <p>
     * @param src the cdx input
     * @param out an {@link Writer} to send the result to
     * @throws IOException is thrown if the underlying IO classes could not read or write
     */
    void reformat(CdxSource src, Writer out) throws IOException {
        out = new BufferedWriter(out);
        SearchResult result = src.search(new SearchKey(), null, false);

        CdxFormat outputFormat;
        switch (format) {
            case "cdxj":
                outputFormat = CdxjLineFormat.DEFAULT_CDXJLINE;
                break;
            case "cdx9":
                outputFormat = CdxLineFormat.CDX09LINE;
                break;
            case "cdx11":
                outputFormat = CdxLineFormat.CDX11LINE;
                break;
            default:
                outputFormat = null;
        }

        CdxRecordFormatter formatter = new CdxRecordFormatter(outputFormat);

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
