/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2020, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.dfs.wrapper.concurrent.strategy;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.dfs.reader.DFSParquetFileReader;
import com.denodo.connect.dfs.util.io.PathIterator;
import com.denodo.connect.dfs.reader.ParquetSchemaHolder;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class NonConcurrentReadingStrategy implements ReadingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(NonConcurrentReadingStrategy.class);


    private final PathIterator pathIterator;
    private final Configuration conf;
    private final MessageType schema;
    private final List<CustomWrapperFieldExpression> projectedFields;
    private final Filter filter;
    private final boolean includePathColumn;
    private final CustomWrapperResult result;
    private final AtomicBoolean stopRequested;

    public NonConcurrentReadingStrategy(final PathIterator pathIterator, final Configuration conf,
        final ParquetSchemaHolder schemaHolder, final List<CustomWrapperFieldExpression> projectedFields,
        final Filter filter, final boolean includePathColumn, final CustomWrapperResult result,
        final AtomicBoolean stopRequested) {

        this.pathIterator = pathIterator;
        this.conf = conf;
        this.schema = schemaHolder.getQuerySchema();
        this.projectedFields = projectedFields;
        this.filter = filter;
        this.includePathColumn = includePathColumn;
        this.result = result;
        this.stopRequested = stopRequested;
    }


    @Override
    public void read() throws IOException {

        final long start = System.currentTimeMillis();

        while (this.pathIterator.hasNext()) {
            final DFSParquetFileReader reader = new DFSParquetFileReader(this.conf, this.pathIterator.next(),
                this.includePathColumn, this.filter, this.schema, this.projectedFields);

            Object parquetData = reader.read();
            while (parquetData != null && !this.stopRequested.get()) {
                this.result.addRow((Object[]) parquetData, this.projectedFields);
                parquetData = reader.read();
            }

            final long end = System.currentTimeMillis();

            if (LOG.isTraceEnabled()) {
                LOG.trace("Elapsed time " + (end - start) + " ms.");
            }
        }
    }
}