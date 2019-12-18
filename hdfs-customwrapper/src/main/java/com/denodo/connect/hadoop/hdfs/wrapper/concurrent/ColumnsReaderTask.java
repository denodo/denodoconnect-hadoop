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
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
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

/*    *//*
     * Queues generally do not allow insertion of null element, use this object to represent null values
     *//*
    static final Object NULL_VALUE = new Object();*/
    /*
     * Last item placed on the queue to notify the RecordsAssemblerTask the Readers are done
     */
    static final Object[] LAST_VALUE = new Object[0];

    private final Configuration conf;
    private final Path path;
    private final MessageType readingSchema;
    private final List<String> conditionFields;
    private final Filter filter;

    private final List<LinkedBlockingQueue<Object[]>> resultColumns;
    private final int readerTaskIndex;
    private final int skippedColumnIndex;

    public ColumnsReaderTask(final Configuration conf, final Path path, final MessageType readingSchema,
        final MessageType resultsSchema, final List<String> conditionFields, final Filter filter,
        final List<LinkedBlockingQueue<Object[]>> resultColumns, final int readerTaskIndex) {

        // make a copy of Conf because the reading schema is written as a property 'ReadSupport.PARQUET_READ_SCHEMA'
        // in Conf in HDFSParquetFileReader::openReader, and each ColumnsReaderTask have to read a different schema
        this.conf = new Configuration(conf);

        this.path = path;
        this.readingSchema = readingSchema;
        this.conditionFields = conditionFields;
        this.filter = filter;

        this.resultColumns = resultColumns;
        this.readerTaskIndex = readerTaskIndex;
        this.skippedColumnIndex = getSkippedColumnsIndex(readingSchema, resultsSchema);
    }

    private int getSkippedColumnsIndex(final MessageType readingSchema, final MessageType resultsSchema) {
        return readingSchema.getFieldCount() - resultsSchema.getFieldCount();
    }


    @Override
    public Void call() throws IOException {

        if (LOG.isTraceEnabled()) {
            LOG.trace("Starting task in " + Thread.currentThread().getName());
        }
        final long start = System.currentTimeMillis();
        long rowCount = 0;

        // includePathValues is always false, it is returned to VDP by the RecordsAssemblerTask as it has always
        // the same value for each file
        final HDFSParquetFileReader reader = new HDFSParquetFileReader(this.conf, this.path, false,
            this.filter, this.readingSchema, this.conditionFields, null, null, null);
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

    private void storeData(final Object[] parquetData) {

        Object[] values = parquetData;
        if (this.skippedColumnIndex != 0) {
            values = Arrays.copyOf(parquetData, this.skippedColumnIndex);
        }
        this.resultColumns.get(this.readerTaskIndex).add(values);
    }

    private void storeLastResult() {
        this.resultColumns.get(this.readerTaskIndex).add(LAST_VALUE);
    }

}