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

/**
 * Implementations of this interface are commands for the CLI.
 * <p>
 * The CLI uses <a href="http://jcommander.org">http://jcommander.org</a> for parsing command line parameters. Thus the
 * following apply:
 * <p>
 * An implementation of this interface must be annotated with the following annotation on the class:
 * <p>
 * {@code @Parameters(commandNames = <command name>, commandDescription = <Description of the command. Used in help>)}
 * <p>
 * If the command is to take any parameters, these are implemented as annotated fields in the command class as described
 * in <a href="http://jcommander.org">http://jcommander.org</a>.
 * <p>
 * <em>Example:</em>
 * <pre><code>
 * {@literal  @}Parameters(commandNames = "mycmd", commandDescription = "Help for mycmd")}
 *   public class MyCommand implements Command {
 *    {@literal @}Parameter(names = "-debug", description = "Debug mode")
 *     private boolean debug = false;
 *
 *    {@literal @}Override
 *     void exec(MainParameters mp) {
 *       if (debug) {
 *         ...
 *       }
 *     }
 *   }
 * </code></pre>
 */
public interface Command {

    /**
     * Implementation of the command.
     * <p>
     * Called by the {@link Main} class with the global parameters.
     * <p>
     * @param mp the parsed global parameters.
     * @throws java.lang.Exception is thrown if command fails execution
     */
    void exec(MainParameters mp) throws Exception;

}
