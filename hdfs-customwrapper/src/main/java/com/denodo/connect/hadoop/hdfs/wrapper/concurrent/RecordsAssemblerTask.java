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

import static com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ColumnsReaderTask.LAST_VALUE;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

/**
 * It is a Callable<Void> instead a Runnable because Callable can throw checked exceptions.
 *
 */
public final class RecordsAssemblerTask implements Callable<Void> {


    private final CustomWrapperResult vdpResult;
    private final List<CustomWrapperFieldExpression> projectedFields;
    private final List<LinkedBlockingQueue<Object[]>> resultColumns;
    private final String fullPathColumn;
    private final boolean invokeAddRow;

    public RecordsAssemblerTask(final CustomWrapperResult vdpResult, final List<CustomWrapperFieldExpression> projectedFields,
        final List<LinkedBlockingQueue<Object[]>> resultColumns, final String fullPathColumn, final boolean invokeAddRow) {

        this.vdpResult = vdpResult;
        this.projectedFields = projectedFields;
        this.resultColumns = resultColumns;
        this.fullPathColumn = fullPathColumn;
        this.invokeAddRow = invokeAddRow;
    }

    @Override
    public Void call() throws InterruptedException {

        final int size = this.projectedFields.size();

        while (true) {

            final Object[] row = new Object[size];
            int i = 0;
            for (final LinkedBlockingQueue<Object[]> resultColumn : this.resultColumns) {
                final Object[] values = resultColumn.take();
                if (LAST_VALUE == values) {
                    return null;
                }
                for (int j = 0; j < values.length; j++) {
                    row[i++] = values[j];
                }

            }

            if (this.fullPathColumn != null) {
                row[i] = this.fullPathColumn;
            }

            if (this.invokeAddRow) {
                this.vdpResult.addRow(row, this.projectedFields);
            }

        }

    }

}