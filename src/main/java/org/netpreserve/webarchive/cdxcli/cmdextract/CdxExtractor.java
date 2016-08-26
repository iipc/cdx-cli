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

import org.netpreserve.commons.cdx.cdxrecord.UnconnectedCdxRecord;

import java.io.BufferedInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import org.jwat.arc.ArcHeader;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecord;
import org.jwat.arc.ArcRecordBase;
import org.jwat.archive.FileIdent;
import org.jwat.common.HttpHeader;
import org.jwat.common.UriProfile;
import org.jwat.gzip.GzipEntry;
import org.jwat.gzip.GzipReader;
import org.jwat.warc.WarcConstants;
import org.jwat.warc.WarcHeader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.netpreserve.commons.cdx.FieldName;
import org.netpreserve.commons.cdx.json.NumberValue;
import org.netpreserve.commons.cdx.json.StringValue;
import org.netpreserve.commons.cdx.json.TimestampValue;
import org.netpreserve.commons.cdx.json.UriValue;

/**
 * Extract CDX records from ARC and WARC files.
 */
public class CdxExtractor {

    /**
     * URI profile to use.
     */
    private final UriProfile uriProfile = UriProfile.RFC3986_ABS_16BIT_LAX;

    /**
     * Enable block digest calculation/validation.
     */
    private final boolean blockDigestEnabled = true;

    /**
     * Enable payload digest calculation/validation.
     */
    private final boolean payloadDigestEnabled = true;

    /**
     * Max record header size.
     */
    private final int recordHeaderMaxSize = 8192;

    /**
     * Max payload header size (http header etc.).
     */
    private final int payloadHeaderMaxSize = 32768;

    public void process(File inFile, FileIdent fileIdent, Output out) {
        String fileName = inFile.getName();

        if (fileIdent.streamId == FileIdent.FILEID_ARC
                || fileIdent.streamId == FileIdent.FILEID_ARC_GZ) {

            if (fileIdent.streamId == FileIdent.FILEID_ARC_GZ) {
                try (InputStream input = new BufferedInputStream(new FileInputStream(inFile), 1024 * 512);
                        ArcReader arcReader = ArcReaderFactory.getReaderUncompressed();) {

                    configureArcReader(arcReader);
                    processArcGzipStream(arcReader, input, fileName, out);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            } else {
                try (InputStream input = new BufferedInputStream(new FileInputStream(inFile), 1024 * 512);
                        ArcReader arcReader = ArcReaderFactory.getReaderUncompressed(input);) {

                    configureArcReader(arcReader);
                    processArcStream(arcReader, fileName, out);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        } else if (fileIdent.streamId == FileIdent.FILEID_WARC
                || fileIdent.streamId == FileIdent.FILEID_WARC_GZ) {

            if (fileIdent.streamId == FileIdent.FILEID_WARC_GZ) {
                try (InputStream input = new BufferedInputStream(new FileInputStream(inFile), 1024 * 512);
                        WarcReader warcReader = WarcReaderFactory.getReaderUncompressed();) {

                    configureWarcReader(warcReader);
                    processWarcGzipStream(warcReader, input, fileName, out);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            } else {
                try (InputStream input = new BufferedInputStream(new FileInputStream(inFile), 1024 * 512);
                        WarcReader warcReader = WarcReaderFactory.getReaderUncompressed(input);) {

                    configureWarcReader(warcReader);
                    processWarcStream(warcReader, fileName, out);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        }
    }

    private void processArcStream(ArcReader arcReader, String fileName, Output out)
            throws IOException {

        long offset = 0;
        ArcRecordBase arcRecord;
        while ((arcRecord = arcReader.getNextRecord()) != null) {
            UnconnectedCdxRecord currentRecord = readArcRecord(arcRecord, fileName);
            if (currentRecord != null) {
                currentRecord.set(FieldName.RECORD_LENGTH, NumberValue.valueOf(arcRecord.getConsumed()));
                offset += arcRecord.getConsumed();

                out.write(currentRecord);
            }
        }
    }

    private void processArcGzipStream(ArcReader arcReader, InputStream input, String fileName, Output out)
            throws IOException {

        try (GzipReader gzipReader = new GzipReader(input);) {
            GzipEntry gzipEntry;
            while ((gzipEntry = gzipReader.getNextEntry()) != null) {
                try (InputStream in = gzipEntry.getInputStream();) {
                    ArcRecordBase arcRecord;
                    while ((arcRecord = arcReader.getNextRecordFrom(in, gzipEntry.getStartOffset()))
                            != null) {

                        UnconnectedCdxRecord currentRecord = readArcRecord(arcRecord, fileName);
                        if (currentRecord != null) {
                            gzipEntry.close();
                            currentRecord.set(FieldName.RECORD_LENGTH, NumberValue.valueOf(gzipEntry.consumed));

                            out.write(currentRecord);
                        }
                    }
                }
            }
        }
    }

    private void processWarcStream(WarcReader warcReader, String fileName, Output out)
            throws IOException {

        WarcRecord warcRecord;
        while ((warcRecord = warcReader.getNextRecord()) != null) {
            UnconnectedCdxRecord currentRecord = readWarcRecord(warcRecord, fileName);
            if (currentRecord != null) {
                currentRecord.set(FieldName.RECORD_LENGTH, NumberValue.valueOf(warcRecord.getConsumed()));

                out.write(currentRecord);
            }
        }
    }

    private void processWarcGzipStream(WarcReader warcReader, InputStream input, String fileName, Output out)
            throws IOException {
        try (GzipReader gzipReader = new GzipReader(input);) {
            GzipEntry gzipEntry;
            while ((gzipEntry = gzipReader.getNextEntry()) != null) {
                try (InputStream in = gzipEntry.getInputStream();) {
                    WarcRecord warcRecord;
                    while ((warcRecord = warcReader.getNextRecordFrom(in, gzipEntry.getStartOffset()))
                            != null) {

                        UnconnectedCdxRecord currentRecord = readWarcRecord(warcRecord, fileName);
                        if (currentRecord != null) {
                            gzipEntry.close();
                            currentRecord.set(FieldName.RECORD_LENGTH, NumberValue.valueOf(gzipEntry.consumed));

                            out.write(currentRecord);
                        }
                    }
                }
            }
        }
    }

    private UnconnectedCdxRecord readArcRecord(ArcRecordBase arcRecord, String fileName) throws IOException {
        if (arcRecord.recordType != ArcRecord.RT_ARC_RECORD) {
            return null;
        }

        UnconnectedCdxRecord currentRecord = new UnconnectedCdxRecord();
        currentRecord.set(FieldName.FILENAME, StringValue.valueOf(fileName));

        ArcHeader arcHeader = arcRecord.header;
        currentRecord.set(FieldName.TIMESTAMP, TimestampValue.valueOf(arcHeader.archiveDateStr));
        currentRecord.set(FieldName.ORIGINAL_URI, UriValue.valueOf(arcHeader.urlStr));
        currentRecord.set(FieldName.RECORD_TYPE, StringValue.valueOf("response"));
        currentRecord.set(FieldName.OFFSET, NumberValue.valueOf(arcRecord.getStartOffset()));

        String mimeType = arcHeader.contentTypeStr;
        long length = arcHeader.archiveLength;
        if (arcRecord.getHttpHeader() != null) {
            HttpHeader httpHeader = arcRecord.getHttpHeader();
            if (httpHeader.isValid()) {
                length = httpHeader.getPayloadLength();
            }
            mimeType = httpHeader.contentType;
            currentRecord.set(FieldName.RESPONSE_CODE, NumberValue
                    .valueOf(httpHeader.getProtocolStatusCode()));
        }

        currentRecord.set(FieldName.CONTENT_TYPE, StringValue.valueOf(mimeType));
        currentRecord.set(FieldName.CONTENT_LENGTH, NumberValue.valueOf(arcHeader.archiveLength));
        currentRecord.set(FieldName.PAYLOAD_LENGTH, NumberValue.valueOf(length));
        arcRecord.close();
//        currentRecord.set(FieldName.DIGEST, StringValue.valueOf(arcRecord.computedBlockDigest.digestString));

        if (arcRecord.computedPayloadDigest != null) {
            currentRecord.set(FieldName.PAYLOAD_DIGEST,
                    StringValue.valueOf(arcRecord.computedPayloadDigest.digestString));
        }
        return currentRecord;
    }

    private UnconnectedCdxRecord readWarcRecord(WarcRecord warcRecord, String fileName) throws IOException {
        if (warcRecord.header.warcTypeIdx == WarcConstants.RT_IDX_WARCINFO
                || warcRecord.header.warcTypeIdx == WarcConstants.RT_IDX_METADATA) {
            return null;
        }

        UnconnectedCdxRecord currentRecord = new UnconnectedCdxRecord();
        currentRecord.set(FieldName.FILENAME, StringValue.valueOf(fileName));

        WarcHeader warcHeader = warcRecord.header;
        currentRecord.set(FieldName.RECORD_ID, StringValue.valueOf(warcHeader.warcRecordIdStr));
        currentRecord.set(FieldName.TIMESTAMP, TimestampValue.valueOf(warcHeader.warcDateStr));
        currentRecord.set(FieldName.ORIGINAL_URI, UriValue.valueOf(warcHeader.warcTargetUriStr));
        currentRecord.set(FieldName.RECORD_TYPE, StringValue.valueOf(warcHeader.warcTypeStr));
        currentRecord.set(FieldName.OFFSET, NumberValue.valueOf(warcRecord.getStartOffset()));

        String mimeType = warcHeader.contentTypeStr;
        long length = warcHeader.contentLength;
        if (warcRecord.getHttpHeader() != null) {
            HttpHeader httpHeader = warcRecord.getHttpHeader();
            if (httpHeader.isValid()) {
                length = httpHeader.getPayloadLength();
            }
            mimeType = httpHeader.contentType;
            currentRecord.set(FieldName.RESPONSE_CODE, NumberValue
                    .valueOf(httpHeader.getProtocolStatusCode()));
        }

        currentRecord.set(FieldName.CONTENT_TYPE, StringValue.valueOf(mimeType));
        currentRecord.set(FieldName.CONTENT_LENGTH, NumberValue.valueOf(warcHeader.contentLength));
        currentRecord.set(FieldName.PAYLOAD_LENGTH, NumberValue.valueOf(length));
        warcRecord.close();
//        currentRecord.set(FieldName.DIGEST, StringValue.valueOf(warcRecord.computedBlockDigest.digestString));

        if (warcRecord.computedPayloadDigest != null) {
            currentRecord.set(FieldName.PAYLOAD_DIGEST,
                    StringValue.valueOf(warcRecord.computedPayloadDigest.digestString));
        }

        if (warcRecord.header.warcTypeIdx == WarcConstants.RT_IDX_REVISIT) {
            currentRecord.set(FieldName.REVISIT_ORIGINAL_ID, StringValue.valueOf(warcHeader.warcRefersToStr));
            currentRecord.set(FieldName.REVISIT_ORIGINAL_URI, StringValue.valueOf(warcHeader.warcRefersToTargetUriStr));
            currentRecord.set(FieldName.REVISIT_ORIGINAL_DATE, StringValue.valueOf(warcHeader.warcRefersToDateStr));
        }

        return currentRecord;
    }

    private void configureArcReader(ArcReader arcReader) {
        arcReader.setUriProfile(uriProfile);
        arcReader.setBlockDigestEnabled(blockDigestEnabled);
        arcReader.setBlockDigestAlgorithm("SHA1");
        arcReader.setBlockDigestEncoding("base32");
        arcReader.setPayloadDigestEnabled(payloadDigestEnabled);
        arcReader.setPayloadDigestAlgorithm("SHA1");
        arcReader.setPayloadDigestEncoding("base32");
        arcReader.setRecordHeaderMaxSize(recordHeaderMaxSize);
        arcReader.setPayloadHeaderMaxSize(payloadHeaderMaxSize);
    }

    private void configureWarcReader(WarcReader warcReader) {
        warcReader.setWarcTargetUriProfile(uriProfile);
        warcReader.setBlockDigestEnabled(blockDigestEnabled);
        warcReader.setBlockDigestAlgorithm("SHA1");
        warcReader.setBlockDigestEncoding("base32");
        warcReader.setPayloadDigestEnabled(payloadDigestEnabled);
        warcReader.setPayloadDigestAlgorithm("SHA1");
        warcReader.setPayloadDigestEncoding("base32");
        warcReader.setRecordHeaderMaxSize(recordHeaderMaxSize);
        warcReader.setPayloadHeaderMaxSize(payloadHeaderMaxSize);
    }

}
