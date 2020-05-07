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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.dfs.reader.ParquetSchemaHolder;
import com.denodo.connect.dfs.util.io.PathIterator;
import com.denodo.connect.dfs.wrapper.concurrent.ReaderManager;
import com.denodo.connect.dfs.wrapper.concurrent.ReaderTask;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class FileReadingStrategy implements ReadingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(FileReadingStrategy.class);


    private final PathIterator pathIterator;
    private final Configuration conf;
    private final MessageType schema;
    private final List<CustomWrapperFieldExpression> projectedFields;
    private final Filter filter;
    private final boolean includePathColumn;
    private final CustomWrapperResult result;
    private final int parallelismLevel;
    private final ReaderManager readerManager;
    private final AtomicBoolean stopRequested;

    public FileReadingStrategy(final PathIterator pathIterator, final Configuration conf,
        final ParquetSchemaHolder schemaHolder, final List<CustomWrapperFieldExpression> projectedFields,
        final Filter filter, final boolean includePathColumn, final CustomWrapperResult result, final int parallelismLevel,
        final ReaderManager readerManager, final AtomicBoolean stopRequested) {

        this.pathIterator = pathIterator;
        this.conf = conf;
        this.schema = schemaHolder.getQuerySchema();
        this.projectedFields = projectedFields;
        this.filter = filter;
        this.includePathColumn = includePathColumn;
        this.result = result;
        this.parallelismLevel = parallelismLevel;
        this.readerManager = readerManager;
        this.stopRequested = stopRequested;
    }

    @Override
    public void read() throws ExecutionException {

        final long start = System.currentTimeMillis();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Reading by file with parallelism level " + this.parallelismLevel);
        }

        final Collection<Callable> readers = new ArrayList<>(this.parallelismLevel);
        while (this.pathIterator.hasNext() && !this.stopRequested.get()) {

            int i = 0;
            while (this.pathIterator.hasNext() && i < this.parallelismLevel) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Reader task: " + i);
                }
                readers.add(new ReaderTask(this.conf, this.pathIterator.next(), this.schema, this.includePathColumn,
                    this.filter, this.projectedFields, this.result));
                i++;
            }

            if (!this.stopRequested.get()) {
                this.readerManager.execute(readers);
                readers.clear();
            }
        }

        final long end = System.currentTimeMillis();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Elapsed time " + (end - start) + " ms.");
        }
    }

}