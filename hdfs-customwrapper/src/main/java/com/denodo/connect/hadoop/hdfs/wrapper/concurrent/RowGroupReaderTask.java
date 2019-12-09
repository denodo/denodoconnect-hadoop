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

import com.denodo.connect.hadoop.hdfs.reader.HDFSParquetFileReader;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * It is a Callable<Void> instead a Runnable because Callable can throw checked exceptions.
 *
 */
public final class RowGroupReaderTask implements Callable<Void> {

    private final Configuration conf;
    private final Path path;
    private final MessageType schema;
    private final boolean includePathColumn;
    private final List<String> conditionFields;
    private final Filter filter;
    private final List<CustomWrapperFieldExpression> projectedFields;
    private final CustomWrapperResult result;
    private final List<BlockMetaData> rowGroups;

    public RowGroupReaderTask(final Configuration conf, final Path path, final MessageType schema, final boolean includePathColumn,
        final List<String> conditionFields, final Filter filter, final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperResult result, final List<BlockMetaData> rowGroups) {

        this.conf = conf;
        this.path = path;
        this.schema = schema;
        this.includePathColumn = includePathColumn;
        this.conditionFields = conditionFields;
        this.filter = filter;
        this.projectedFields = projectedFields;
        this.result = result;
        this.rowGroups = rowGroups;
    }

    @Override
    public Void call() throws IOException {

        int lastRowGroup = rowGroups.size() - 1;
        Long startingPos = rowGroups.get(0).getStartingPos();
        Long endingPos = rowGroups.get(lastRowGroup).getStartingPos() + rowGroups.get(lastRowGroup).getTotalByteSize();

        final HDFSParquetFileReader reader = new HDFSParquetFileReader(this.conf, this.path,
            this.includePathColumn, this.filter, this.schema, this.conditionFields, startingPos, endingPos);

        Object parquetData = reader.read();

        while (parquetData != null ) {

            synchronized (this.result) {
                this.result.addRow((Object[]) parquetData, this.projectedFields);
            }

            parquetData = reader.read();
        }

        return null;
    }
}