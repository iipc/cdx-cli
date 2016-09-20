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
package org.netpreserve.webarchive.cdxcli;

import java.io.FileNotFoundException;

import org.netpreserve.webarchive.cdxcli.cmdreformat.CommandReformat;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.netpreserve.webarchive.cdxcli.cmdextract.CommandExtract;

/**
 * Main class for cdx command line tool.
 */
public final class Main {

    /**
     * Private constructor to avoid instantiation.
     */
    private Main() {
    }

    /**
     * Main method.
     * <p>
     * @param args command line arguments
     */
    public static void main(String[] args) {
        MainParameters mp = new MainParameters();
        JCommander jc = new JCommander(mp);
        jc.setProgramName("cdxcli");
        jc.addConverterFactory(new FormatConverterFactory());

        jc.addCommand(new CommandReformat());
        jc.addCommand(new CommandExtract());

        try {
            jc.parse(args);

            if (mp.version) {
                System.out.println("cdxcli version: " + Main.class.getPackage().getImplementationVersion());
                System.exit(0);
            }

            if (mp.help || jc.getParsedCommand() == null) {
                jc.usage();
                System.exit(0);
            }

            if (mp.workDir != null) {
                Path workPath = Paths.get(mp.workDir);
                if (!Files.isDirectory(workPath)) {
                    throw new FileNotFoundException("Workdir '" + workPath.toAbsolutePath() + "' does not exist");
                }
                System.setProperty("java.io.tmpdir", mp.workDir);
            }

            String command = jc.getParsedCommand();
            long startTime = System.currentTimeMillis();

            ((Command) jc.getCommands().get(command).getObjects().get(0)).exec(mp);

            long runTime = System.currentTimeMillis() - startTime;
            System.err.println("Command " + command + " was executed in " + runTime + "ms");

        } catch (ParameterException e) {
            System.err.println(e.getLocalizedMessage());
            if (mp.printStacktrace) {
                e.printStackTrace();
            }
            jc.usage();
            System.exit(1);
        } catch (UncheckedIOException e) {
            System.err.println(e.getCause().getLocalizedMessage());
            if (mp.printStacktrace) {
                e.printStackTrace();
            }
            System.exit(2);
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            if (mp.printStacktrace) {
                e.printStackTrace();
            }
            System.exit(3);
        }
    }

}
