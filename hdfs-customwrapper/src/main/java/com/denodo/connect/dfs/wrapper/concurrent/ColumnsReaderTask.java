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
package com.denodo.connect.dfs.wrapper.concurrent;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.dfs.reader.DFSParquetFileReader;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

/**
 * It is a Callable<Void> instead a Runnable because Callable can throw checked exceptions.
 *
 */
public final class ColumnsReaderTask implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnsReaderTask.class);

    private final int readerIndex;
    private final Configuration conf;
    private final Path path;
    private final MessageType readingSchema;
    private final boolean includePathValue;
    private final Filter filter;
    private final List<CustomWrapperFieldExpression> projectedFields;

    private final ColumnGroupReadingStructure readingStructure;

    private final AtomicBoolean stopRequested;


    public ColumnsReaderTask(final int readerIndex, final Configuration conf, final Path path, final MessageType readingSchema,
        final boolean includePathValue, final Filter filter, final List<CustomWrapperFieldExpression> projectedFields,
        final ColumnGroupReadingStructure readingStructure, final AtomicBoolean stopRequested) {

        super();

        this.readerIndex = readerIndex;

        // make a copy of Conf because the reading schema is written as a property 'ReadSupport.PARQUET_READ_SCHEMA'
        // in Conf in HDFSParquetFileReader::openReader, and each ColumnsReaderTask have to read a different schema
        this.conf = new Configuration(conf);

        this.path = path;
        this.readingSchema = readingSchema;
        this.includePathValue = includePathValue;
        this.filter = filter;
        this.projectedFields = projectedFields;

        this.readingStructure = readingStructure;

        this.stopRequested = stopRequested;

    }


    @Override
    public Void call() throws IOException, InterruptedException {

        if (LOG.isTraceEnabled()) {
            LOG.trace("Starting task in " + Thread.currentThread().getName());
        }
        final long start = System.currentTimeMillis();
        long rowCount = 0;

        final DFSParquetFileReader reader = new DFSParquetFileReader(this.conf, this.path, this.includePathValue,
            this.filter, this.readingSchema, this.projectedFields);

        if (this.stopRequested.get()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("READER[%d] READING CANCELLED ON USER REQUEST", this.readerIndex));
            }
            return null;
        }

        Object parquetData = reader.read();
        while (parquetData != null && !this.stopRequested.get()) {

            // Try to obtain a buffer chunk, using a timeout, and check for stop requests while trying
            Object[][] chunk = null;
            do {
                chunk = this.readingStructure.acquireBufferChunk();
            } while (chunk == null && !this.stopRequested.get());

            if (chunk != null) {

                int chunkIndex = 0;
                while (parquetData != null && chunkIndex < chunk.length && !this.stopRequested.get()) {

                    chunk[chunkIndex++] = (Object[]) parquetData;

                    rowCount++;

                    parquetData = reader.read();

                }

                // Publish data, but checking for stop requests using a timeout
                while(!this.readingStructure.publishDataChunk(chunk) && !this.stopRequested.get());

            }

        }

        if (this.stopRequested.get()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("READER[%d] READING CANCELLED ON USER REQUEST", this.readerIndex));
            }
            return null;
        }

        this.readingStructure.signalNoMoreData();

        final long end = System.currentTimeMillis();

        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    String.format(
                        "READER[%d] FINISHED IN %dms. TOTAL RETURNED: %d.",
                        this.readerIndex, (end-start), rowCount));
        }

        return null;
    }

}