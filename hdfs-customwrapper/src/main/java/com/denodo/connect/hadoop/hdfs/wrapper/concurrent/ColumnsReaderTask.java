/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2019, denodo technologies (http://www.denodo.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * =============================================================================
 */
package com.denodo.connect.hadoop.hdfs.wrapper.concurrent;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.reader.HDFSParquetFileReader;

/**
 * It is a Callable<Void> instead a Runnable because Callable can throw checked exceptions.
 *
 */
public final class ColumnsReaderTask implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnsReaderTask.class);

    /*
     * Last item placed on the queue to notify the RecordsAssemblerTask the Readers are done
     */
    static final Object LAST_VALUE = new Object();

    private final Configuration conf;
    private final Path path;
    private final MessageType readingSchema;
    private final List<String> conditionFields;
    private final Filter filter;
    private ParquetMetadata parquetMetadata;

    private final List<BlockingQueue<Object[]>> resultColumns;
    private final int readerTaskIndex;
    private final int skippedColumnIndex;


    public ColumnsReaderTask(final Configuration conf, final Path path, final MessageType readingSchema,
        final MessageType resultsSchema, final List<String> conditionFields, final Filter filter,
        final List<BlockingQueue<Object[]>> resultColumns, final int readerTaskIndex, final ParquetMetadata parquetMetadata) {

        // make a copy of Conf because the reading schema is written as a property 'ReadSupport.PARQUET_READ_SCHEMA'
        // in Conf in HDFSParquetFileReader::openReader, and each ColumnsReaderTask have to read a different schema
        this.conf = new Configuration(conf);

        this.path = path;
        this.readingSchema = readingSchema;
        this.conditionFields = conditionFields;
        this.filter = filter;
        this.parquetMetadata = parquetMetadata;

        this.resultColumns = resultColumns;
        this.readerTaskIndex = readerTaskIndex;
        this.skippedColumnIndex = getSkippedColumnsIndex(readingSchema, resultsSchema);
    }

    private int getSkippedColumnsIndex(final MessageType readingSchema, final MessageType resultsSchema) {

        int skippedColumnIndex = 0;
        final int fieldDifference = readingSchema.getFieldCount() - resultsSchema.getFieldCount();
        if (fieldDifference > 0) {
            skippedColumnIndex = readingSchema.getFieldCount() - fieldDifference;
        }

        return skippedColumnIndex;
    }


    @Override
    public Void call() throws IOException, InterruptedException {

        if (LOG.isTraceEnabled()) {
            LOG.trace("Starting task in " + Thread.currentThread().getName());
        }
        final long start = System.currentTimeMillis();
        long rowCount = 0;

        // includePathValues is always false, it is returned to VDP by the RecordsAssemblerTask as it has always
        // the same value for each file
        final HDFSParquetFileReader reader = new HDFSParquetFileReader(this.conf, this.path, false,
            this.filter, this.readingSchema, this.conditionFields, null, null, this.parquetMetadata);
        Object parquetData = reader.read();
        while (parquetData != null ) {

            storeData((Object[]) parquetData);

            rowCount++;

            parquetData = reader.read();
        }


        storeLastResult();

        final long end = System.currentTimeMillis();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Elapsed time " + (end - start) + " thread" + Thread.currentThread().getName());
            LOG.trace("Ending task in " + Thread.currentThread().getName() + " ; total rows " + rowCount);
        }

        return null;
    }

    private void storeData(final Object[] parquetData) throws InterruptedException {

        Object[] values = parquetData;
        if (this.skippedColumnIndex != 0) {
            values = Arrays.copyOf(parquetData, this.skippedColumnIndex);
        }
        this.resultColumns.get(this.readerTaskIndex).put(values);
    }

    private void storeLastResult() throws InterruptedException {

        int size = this.skippedColumnIndex;
        if (this.skippedColumnIndex == 0) {
            size = this.readingSchema.getFieldCount();
        }

        final Object[] lastValueArray = new Object[size];
        for (int i = 0; i < size; i ++) {
            lastValueArray[i] = LAST_VALUE;
        }

        this.resultColumns.get(this.readerTaskIndex).put(lastValueArray);
    }

}