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
import java.io.IOException;

import org.netpreserve.commons.cdx.CdxRecord;
import org.netpreserve.commons.cdx.formatter.CdxRecordFormatter;
import org.netpreserve.commons.cdx.sort.CloseableStringQueue;
import org.netpreserve.commons.cdx.sort.PolyphaseMergeSort;

/**
 *
 */
public class SortingOutput implements Output {

    private final BufferedWriter writer;

    private final CdxRecordFormatter formatter;

    private final CloseableStringQueue queue;

    private final PolyphaseMergeSort pms;

    private final Thread sortingThread;
    private long count;

    public SortingOutput(BufferedWriter writer, CdxRecordFormatter formatter, int scratchFileCount, int heapSize) {
        this.writer = writer;
        this.formatter = formatter;
        this.queue = new CloseableStringQueue(128);
        this.pms = new PolyphaseMergeSort(scratchFileCount, heapSize);
        sortingThread = new Thread(new SortingThread(), "Sorting thread");
        sortingThread.start();
    }

    @Override
    public void write(CdxRecord record) {
        try {
            queue.put(formatter.format(record, true));
            count++;
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        queue.close();
        try {
            // Wait for sort to finish
            sortingThread.join();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        writer.close();
    }

    /**
     * The thread executing the sort.
     */
    private class SortingThread implements Runnable {

        @Override
        public void run() {
            pms.sort(queue, writer);
        }

    }
}
