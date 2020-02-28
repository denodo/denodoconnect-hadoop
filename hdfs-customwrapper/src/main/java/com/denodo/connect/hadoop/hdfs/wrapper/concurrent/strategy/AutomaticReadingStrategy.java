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
package com.denodo.connect.hadoop.hdfs.wrapper.concurrent.strategy;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.util.io.PathIterator;
import com.denodo.connect.hadoop.hdfs.reader.ParquetSchemaHolder;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ReaderManagerFactory;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperNotCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class AutomaticReadingStrategy implements ReadingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(AutomaticReadingStrategy.class);


    private static final int COLS_THRESHOLD = 3;
    private static final int ROWGROUPS_THRESHOLD = 10;
    private static final int ROWGROUP_ROWS_THRESHOLD = 1000;

    private final PathIterator pathIterator;
    private final Configuration conf;
    private final ParquetSchemaHolder parquetSchemaHolder;
    private final List<CustomWrapperFieldExpression> projectedFields;
    private final Filter filter;
    private final boolean includePathColumn;
    private final CustomWrapperResult result;
    private final int parallelism;
    private final String fileSystemURI;
    private final int threadPoolSize;

    private final String clusteringFields;
    private final int numCols;
    private final List<BlockMetaData> rowGroups;
    private final int numRowGroups;
    private final boolean rootIsDir;
    private final CustomWrapperCondition wrapperCondition;

    private final AtomicBoolean stopRequested;

    public AutomaticReadingStrategy(final PathIterator pathIterator, final Configuration conf,
        final ParquetSchemaHolder schemaHolder, final List<CustomWrapperFieldExpression> projectedFields,
        final Filter filter, final boolean includePathColumn, final CustomWrapperResult result, final int parallelism,
        final String fileSystemURI, final int threadPoolSize, final String clusteringFields, final boolean rootIsDir,
        final CustomWrapperCondition wrapperCondition, final AtomicBoolean stopRequested) {

        this.pathIterator = pathIterator;
        this.conf = conf;
        this.parquetSchemaHolder = schemaHolder;
        this.projectedFields = projectedFields;
        this.filter = filter;
        this.includePathColumn = includePathColumn;
        this.result = result;
        this.parallelism = parallelism;
        this.fileSystemURI = fileSystemURI;
        this.threadPoolSize = threadPoolSize;

        this.clusteringFields = clusteringFields;
        this.numCols = schemaHolder.getQuerySchema().getColumns().size();
        this.rowGroups = schemaHolder.getRowGroups();
        this.numRowGroups = this.rowGroups.size();
        this.rootIsDir = rootIsDir;
        this.wrapperCondition = wrapperCondition;

        this.stopRequested = stopRequested;
    }


    @Override
    public void read() throws IOException, ExecutionException {

        final ReadingStrategy readingStrategy = selectReadingStrategy();
        readingStrategy.read();

    }

    private ReadingStrategy selectReadingStrategy() {

        ReadingStrategy readingStrategy = null;

        if (this.clusteringFields != null && areRequiredCondition(this.clusteringFields, this.wrapperCondition)  && this.numCols > COLS_THRESHOLD) {
            readingStrategy = new ColumnReadingStrategy(this.pathIterator, this.conf, this.parquetSchemaHolder,
                this.projectedFields, this.filter, this.result, this.parallelism,
                ReaderManagerFactory.get(this.fileSystemURI, this.threadPoolSize), this.stopRequested);

        } else if (this.rootIsDir) { // rootIsDir: naive approach to check if num_files > 1, because 'file name pattern' would affect to the total count
            readingStrategy = new FileReadingStrategy(this.pathIterator, this.conf, this.parquetSchemaHolder,
                this.projectedFields, this.filter, this.includePathColumn, this.result, this.parallelism,
                ReaderManagerFactory.get(this.fileSystemURI, this.threadPoolSize), this.stopRequested);

        } else if (this.numRowGroups > 1 && (findAnyGt(this.rowGroups, ROWGROUP_ROWS_THRESHOLD) || this.numRowGroups > ROWGROUPS_THRESHOLD)) {
            readingStrategy = new RowGroupReadingStrategy(this.pathIterator, this.conf, this.parquetSchemaHolder,
                this.projectedFields, this.filter, this.includePathColumn, this.result, this.parallelism,
                ReaderManagerFactory.get(this.fileSystemURI, this.threadPoolSize), this.stopRequested);

        } else if (this.numCols > COLS_THRESHOLD) {
            readingStrategy = new ColumnReadingStrategy(this.pathIterator, this.conf, this.parquetSchemaHolder,
                this.projectedFields, this.filter, this.result, this.parallelism,
                ReaderManagerFactory.get(this.fileSystemURI, this.threadPoolSize), this.stopRequested);

        } else {
            readingStrategy = new NonConcurrentReadingStrategy(this.pathIterator, this.conf, this.parquetSchemaHolder,
                this.projectedFields, this.filter, this.includePathColumn, this.result, this.stopRequested);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Selected " + readingStrategy.getClass().getSimpleName() + " as reading strategy");
        }

        return readingStrategy;

    }

    private static boolean findAnyGt(final List<BlockMetaData> rowGroups, final int rowgroupRowsThreshold) {

        for (final BlockMetaData blockMetaData : rowGroups) {
            if (blockMetaData.getRowCount() > rowgroupRowsThreshold) {
                return true;
            }
        }

        return false;
    }

    private static boolean areRequiredCondition(final String fields, final CustomWrapperCondition wrapperCondition) {

        final String[] fragmentFields = fields.split(",");
        for (final String field : fragmentFields) {
            if (isRequiredCondition(field.trim(), wrapperCondition)) {
                return true;
            }
        }

        return false;
    }

    private static boolean isRequiredCondition(final String field, final CustomWrapperCondition wrapperCondition) {

        final boolean required = false;
        if (wrapperCondition != null) {

            if (wrapperCondition.isSimpleCondition()) {
                final CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) wrapperCondition;

                return field.equals(simpleCondition.getField().toString());

            }  else if (wrapperCondition.isNotCondition()) {
                final CustomWrapperNotCondition notCondition = (CustomWrapperNotCondition) wrapperCondition;
                final CustomWrapperCondition condition = notCondition.getCondition();

                return isRequiredCondition(field, condition);

            } else if (wrapperCondition.isAndCondition()) {
                final CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) wrapperCondition;
                final List<CustomWrapperCondition> conditions = andCondition.getConditions();
                for (final CustomWrapperCondition condition : conditions) {
                    if (isRequiredCondition(field, condition)) {
                        return true;
                    }

                }

            } else if (wrapperCondition.isOrCondition()) {
                final CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) wrapperCondition;
                final List<CustomWrapperCondition> conditions = orCondition.getConditions();
                for (final CustomWrapperCondition condition : conditions) {
                    if (! isRequiredCondition(field, condition)) {
                        return false;
                    }
                }

            }

        }

        return required;

    }
}