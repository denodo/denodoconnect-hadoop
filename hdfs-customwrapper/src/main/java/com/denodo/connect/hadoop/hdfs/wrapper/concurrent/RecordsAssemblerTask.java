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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

/**
 * It is a Callable<Void> instead a Runnable because Callable can throw checked exceptions.
 *
 */
public final class RecordsAssemblerTask implements Callable<Void> {


    private final CustomWrapperResult vdpResult;
    private final List<CustomWrapperFieldExpression> projectedFields;
    private final List<BlockingQueue<Object[]>> resultColumns;
    private int[] columnOffsets;
    private final String fullPathColumn;
    private final boolean invokeAddRow;

    public RecordsAssemblerTask(final CustomWrapperResult vdpResult, final List<CustomWrapperFieldExpression> projectedFields,
        final List<BlockingQueue<Object[]>> resultColumns, final int[] columnOffsets, final String fullPathColumn,
        final boolean invokeAddRow) {

        this.vdpResult = vdpResult;
        this.projectedFields = projectedFields;
        this.resultColumns = resultColumns;
        this.columnOffsets = columnOffsets;
        this.fullPathColumn = fullPathColumn;
        this.invokeAddRow = invokeAddRow;
    }
/*
    @Override
    public Void call() throws InterruptedException {

        final int size = this.projectedFields.size();

        while (true) {

            final Object[] row = new Object[size];
            int i = 0;
            for (final BlockingQueue<Object[]> resultColumn : this.resultColumns) {
                final Object[] values = resultColumn.take();
                if (LAST_VALUE == values) {
                    return null;
                }
                for (final Object value : values) {
                    row[i++] = value;
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
*/

    @Override
    public Void call() throws InterruptedException {

        final int rowSize = this.projectedFields.size();

        final Collection<Object[]> buffer = new ArrayList<>();
        final List<Object[]> rows = new ArrayList<>();
        final List<BitSet> completeRowFlags = new ArrayList<>();

        while (true) {

            int columnIndex = 0;
            for (final BlockingQueue<Object[]> resultColumn : this.resultColumns) {
                blockingDrain(resultColumn, buffer);

                int rowIndex = 0;
                for (final Object[] partialRow : buffer) {

                    final Map<Integer, Object[]> result = findRow(rows, rowIndex, columnIndex, completeRowFlags);
                    final Object[] row;
                    if (result.isEmpty()) {
                        row = new Object[rowSize];
                        rowIndex = rows.size();
                        rows.add(row);
                        // initialize completeRow flag: one bit for each resultColumn,
                        // when all bits are set, then the row is complete and can be sent to VDP
                        completeRowFlags.add(new BitSet(this.resultColumns.size()));
                    } else {
                        final Entry<Integer, Object[]> entry = result.entrySet().iterator().next();
                        row = entry.getValue();
                        rowIndex = entry.getKey();
                    }

                    int cellIndex = this.columnOffsets[columnIndex];
                    for (final Object cellValue : partialRow) {
                        row[cellIndex ++] = cellValue;
                    }

                    completeRowFlags.get(rowIndex).set(columnIndex);

                    rowIndex ++;
                }

                buffer.clear();
                columnIndex ++;

            }

        /*
            TODO
            if (this.fullPathColumn != null) {
                row[i] = this.fullPathColumn;
            }
        */
                final Iterator<Object[]> rowsIterator = rows.iterator();
                final Iterator<BitSet> flagsIterator = completeRowFlags.iterator();
                while (rowsIterator.hasNext() && flagsIterator.hasNext() ) {
                    final Object[] row = rowsIterator.next();
                    final BitSet flag = flagsIterator.next();
                    if (flag.cardinality() == this.resultColumns.size()) { // check complete_row flag
                        if (LAST_VALUE == row[0]) {
                            return null;
                        }
                        if (this.invokeAddRow) {
                            this.vdpResult.addRow(row, this.projectedFields);
                        }
                        rowsIterator.remove(); // remove row when sent to VDP
                        flagsIterator.remove();
                    }
                }



        }

    }

    private Map<Integer, Object[]> findRow(final List<Object[]> rows, final int index, final int columnIndex,
        final List<BitSet> completeRowFlags) {

        Map<Integer, Object[]> result = new HashMap<>();
        Object[] row = null;
        int rowIndex = index;
        while (row == null && rowIndex < rows.size()) {
            row = rows.get(rowIndex);

            if (completeRowFlags.get(rowIndex).get(columnIndex)) {
                row = null;
                rowIndex ++;
            }
        }

        if (row != null) {
            result.put(rowIndex, row);
        }

        return result;
    }

    private static <E> void blockingDrain(final BlockingQueue<E> queue, final Collection<? super E> buffer)
        throws InterruptedException {

        final int added = queue.drainTo(buffer);
        if (added == 0) {
           // final E element = queue.take();
            final E element = queue.poll(1, TimeUnit.MILLISECONDS);
            if (element != null) {
                buffer.add(element);
                queue.drainTo(buffer);

            }
        }

    }
}