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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import org.netpreserve.commons.cdx.CdxFormat;
import org.netpreserve.commons.cdx.cdxrecord.CdxLineFormat;
import org.netpreserve.commons.cdx.cdxrecord.CdxjLineFormat;

/**
 * Converts a string into a known cdx format.
 */
public class FormatConverter implements IStringConverter<CdxFormat> {

    @Override
    public CdxFormat convert(String value) {
        CdxFormat outputFormat;
        switch (value) {
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
                throw new ParameterException("Illegal format '" + value + "'. Allowed values are: cdxj, cdx9, cdx11");
        }
        return outputFormat;
    }

}
