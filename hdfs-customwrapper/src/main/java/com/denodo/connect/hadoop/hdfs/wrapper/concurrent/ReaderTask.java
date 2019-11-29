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
import java.util.List;
import java.util.concurrent.Callable;

import com.denodo.connect.hadoop.hdfs.reader.HDFSParquetFileReader;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

/**
 * It is a Callable<Void> instead a Runnable because Callable can throw checked exceptions.
 *
 */
public class ReaderTask implements Callable<Void> {

    private HDFSParquetFileReader reader;
    private List<CustomWrapperFieldExpression> projectedFields;
    private CustomWrapperResult result;

    public ReaderTask(final HDFSParquetFileReader reader, final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperResult result) {

        this.reader = reader;
        this.projectedFields = projectedFields;
        this.result = result;
    }

    @Override
    public Void call() throws IOException {

        Object parquetData = this.reader.read();
        while (parquetData != null ) {

            synchronized (this) {
                this.result.addRow((Object[]) parquetData, this.projectedFields);
            }

            parquetData = this.reader.read();
        }

        return null;
    }
}