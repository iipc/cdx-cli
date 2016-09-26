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
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    private final Future sortingThread;

    private final ExecutorService executorService;

    public SortingOutput(BufferedWriter writer, CdxRecordFormatter formatter, int scratchFileCount, int heapSize) {
        this.writer = writer;
        this.formatter = formatter;
        this.queue = new CloseableStringQueue(128);
        this.pms = new PolyphaseMergeSort(scratchFileCount, heapSize);
        this.executorService = Executors.newSingleThreadExecutor();
        this.sortingThread = this.executorService.submit(new SortingThread());
    }

    @Override
    public void write(CdxRecord record) {
        try {
            queue.put(formatter.format(record));
        } catch (Exception ex) {
            try {
                close();
            } catch (IOException ex1) {
                throw new RuntimeException(ex1);
            }
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        queue.close();
        try {
            // Wait for sort to finish
            sortingThread.get();
            executorService.shutdown();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof UncheckedIOException) {
                throw (IOException) ex.getCause().getCause();
            } else {
                throw new RuntimeException(ex.getCause());
            }
        } finally {
            try {
                writer.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * The thread executing the sort.
     */
    private class SortingThread implements Runnable {

        @Override
        public void run() {
            try {
                pms.sort(queue, writer);
            } catch (Exception ex) {
                queue.close();
                throw ex;
            }
        }

    }
}
