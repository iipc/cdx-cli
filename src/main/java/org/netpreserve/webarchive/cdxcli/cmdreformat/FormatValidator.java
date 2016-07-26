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

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

/**
 * Class for validating the value of the format parameter.
 */
public class FormatValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        switch (value) {
            case "cdxj":
            case "cdx9":
            case "cdx11":
                break;
            default:
                throw new ParameterException("Illegal format '" + value + "' for parameter " + name
                        + ". Allowed values are: cdxj, cdx9, cdx11");
        }
    }

}
