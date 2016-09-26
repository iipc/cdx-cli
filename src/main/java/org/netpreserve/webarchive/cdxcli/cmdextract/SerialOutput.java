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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;

import org.netpreserve.commons.cdx.CdxRecord;
import org.netpreserve.commons.cdx.formatter.CdxRecordFormatter;

/**
 *
 */
public class SerialOutput implements Output {

    private final Writer writer;

    private final CdxRecordFormatter formatter;

    public SerialOutput(Writer writer, CdxRecordFormatter formatter) {
        this.writer = writer;
        this.formatter = formatter;
    }

    @Override
    public synchronized void write(CdxRecord record) {
        try {
            formatter.format(writer, record);
            writer.append('\n');
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
    @Override
    public void close() {
        try {
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

}
