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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 *
 */
@Parameters(commandNames = "reformat", commandDescription = "Reformat cdx file")
public class CommandReformat implements Command {

    @Parameter(names = {"-s", "--sort"}, description = "sort file after reformatting")
    boolean sort = false;

    @Parameter(names = {"-c", "--concatenate"}, description = "concatenate input files into one file")
    boolean concatenate = false;

    @Override
    public void exec(MainParameters mp) {
        System.out.println(this.toString());
    }

    @Override
    public String toString() {
        return "CommandReformat{" + "sort=" + sort + ", concatenate=" + concatenate + '}';
    }

}
