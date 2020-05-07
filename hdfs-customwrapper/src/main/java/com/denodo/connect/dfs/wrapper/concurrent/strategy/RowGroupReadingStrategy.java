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

import static java.lang.Math.min;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.dfs.wrapper.concurrent.RowGroupReaderTask;
import com.denodo.connect.dfs.reader.ParquetSchemaHolder;
import com.denodo.connect.dfs.util.io.PathIterator;
import com.denodo.connect.dfs.util.schema.ParquetSchemaUtils;
import com.denodo.connect.dfs.wrapper.concurrent.ReaderManager;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class RowGroupReadingStrategy implements ReadingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(RowGroupReadingStrategy.class);


    private final PathIterator pathIterator;
    private final Configuration conf;
    private final MessageType schema;
    private final List<CustomWrapperFieldExpression> projectedFields;
    private final Filter filter;
    private final boolean includePathColumn;
    private final CustomWrapperResult result;
    private final int parallelismLevel;
    private final List<BlockMetaData> rowGroups;
    private final ReaderManager readerManager;
    private final AtomicBoolean stopRequested;

    public RowGroupReadingStrategy(final PathIterator pathIterator, final Configuration conf,
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
        this.rowGroups = schemaHolder.getRowGroups();
        this.readerManager = readerManager;
        this.stopRequested = stopRequested;
    }


    @Override
    public void read() throws ExecutionException, IOException {

        final long start = System.currentTimeMillis();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Reading by rowgroup with parallelism level " + this.parallelismLevel);
        }

        final Collection<Callable> readers = new ArrayList<>(this.parallelismLevel);
        List<BlockMetaData> fileRowGroups = null;
        while (this.pathIterator.hasNext() && !this.stopRequested.get()) {

            final Path currentPath = this.pathIterator.next();

            //We reuse rowGroups value initialized in the schema in the first file of the path
            fileRowGroups = (fileRowGroups == null) ? this.rowGroups
                : ParquetSchemaUtils.getRowGroups(this.conf, currentPath);

            if (! fileRowGroups.isEmpty()) {
                final List<List<BlockMetaData>> rowGroupsList = generateRowGroupsList(fileRowGroups, this.parallelismLevel);

                final Iterator<List<BlockMetaData>> rowGroupListIterator = rowGroupsList.iterator();
                while (rowGroupListIterator.hasNext() && !this.stopRequested.get()) {
                    readers.add(new RowGroupReaderTask(this.conf, currentPath, this.schema, this.includePathColumn,
                        this.filter, this.projectedFields, this.result, rowGroupListIterator.next()));
                }

                if (!this.stopRequested.get()) {
                    this.readerManager.execute(readers);
                    readers.clear();
                }
            }

        }

        final long end = System.currentTimeMillis();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Elapsed time " + (end - start) + " ms.");
        }
    }

    private static List<List<BlockMetaData>> generateRowGroupsList(final List<BlockMetaData> rowGroups,
        final int parallelismLevel) {

        final int numRowGroups = rowGroups.size();
        int totalGroups = parallelismLevel;
        if (numRowGroups < parallelismLevel) {
            totalGroups = numRowGroups;
        }
        final int div = numRowGroups / totalGroups;
        final int mod = numRowGroups % totalGroups;

        final List<List<BlockMetaData>> rowGroupsList = new ArrayList<>(totalGroups);
        for (int i = 0; i < totalGroups; i++) {
            rowGroupsList.add(new ArrayList<>(rowGroups.subList(i * div + min(i, mod), (i + 1) * div + min(i + 1, mod))));
        }

        return rowGroupsList;
    }
}