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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.wrapper.HDFSParquetFileWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;


/**
 * It is a Callable<Void> instead a Runnable because Callable can throw checked exceptions.
 *
 */
public final class RecordsAssemblerTask implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(RecordsAssemblerTask.class);


    private final CustomWrapperResult vdpResult;
    private final List<CustomWrapperFieldExpression> projectedFields;
    private final List<ColumnGroupReadingStructure> readingStructures;
    private int[] columnOffsets;
    private final String fullPathColumn;
    private final boolean invokeAddRow;

    private final AtomicBoolean stopRequested;


    public RecordsAssemblerTask(final CustomWrapperResult vdpResult, final List<CustomWrapperFieldExpression> projectedFields,
        final List<ColumnGroupReadingStructure> readingStructures, final int[] columnOffsets, final String fullPathColumn,
        final boolean invokeAddRow, final AtomicBoolean stopRequested) {

        this.vdpResult = vdpResult;
        this.projectedFields = projectedFields;
        this.readingStructures = readingStructures;
        this.columnOffsets = columnOffsets;
        this.fullPathColumn = fullPathColumn;
        this.invokeAddRow = invokeAddRow;
        this.stopRequested = stopRequested;

    }

    @Override
    public Void call() throws InterruptedException {

        final int rowSize = this.projectedFields.size();

        final Object[][] rows = new Object[HDFSParquetFileWrapper.READING_CHUNK_SIZE][];

        final int numReaders = this.readingStructures.size();

        while (!allFinished(this.readingStructures) && !this.stopRequested.get()) {

            int numRead = 0;

            for (int readerIndex = 0; readerIndex < numReaders && !this.stopRequested.get(); readerIndex++) {

                final ColumnGroupReadingStructure readingStructure = this.readingStructures.get(readerIndex);

                if (readingStructure.isFinished()) {
                    // No need to check for new values on a queue that no one is writing to
                    continue;
                }

                // Try to obtain data, using a timeout, and check for stop requests while trying
                Object[][] data = null;
                do {
                    data = readingStructure.acquireDataChunk();
                } while (data == null && !this.stopRequested.get());

                if (data != null) {

                    int numReadByReader = 0;
                    Object[] partialRow;
                    for (int i = 0; i < data.length && (partialRow = data[i]) != null; i++) {

                        Object[] row = rows[i];
                        if (row == null) {
                            row = new Object[rowSize];
                            rows[i] = row;
                        }

                        System.arraycopy(partialRow, 0, row, this.columnOffsets[readerIndex], partialRow.length);

                        numReadByReader++;

                    }

                    // Return the data chunk, but checking for stop requests using a timeout
                    while(!readingStructure.returnDataChunk(data) && !this.stopRequested.get());

                    if (LOG.isTraceEnabled() && numReadByReader > 0) {
                        LOG.trace("Read " + numReadByReader + " from reader number " + readerIndex);
                        if (readingStructure.isFinished()) {
                            LOG.trace("Finished reading all rows from reader number " + readerIndex);
                        }
                    }

                    if (numRead == 0) {
                        numRead = numReadByReader;
                    } else if (numRead != numReadByReader) {
                        // TODO Stop all other threads!
                        throw new RuntimeException("Some readers returned" + numRead + " while others returned " + numReadByReader);
                    }

                }

            }

            if (this.stopRequested.get()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("ROW ASSEMBLER CANCELLED ON USER REQUEST");
                }
                return null;
            }


            if (numRead > 0) {

                if (this.invokeAddRow) {
                    for (int i = 0; i < numRead; i++) {
                        if (this.fullPathColumn != null) {
                            rows[i][rowSize - 1] = this.fullPathColumn;
                        }
                        this.vdpResult.addRow(rows[i], this.projectedFields);
                    }
                }

                Arrays.fill(rows, null);

                if (LOG.isTraceEnabled()) {
                    LOG.trace("Rows output in this row assembling cycle: " + numRead);
                }

            } else if (LOG.isTraceEnabled()) {
                    LOG.trace("No rows output in this assembling cycle");
            }

        }

        return null;

    }


    private static boolean allFinished(final List<ColumnGroupReadingStructure> readingStructures) {
        for (final ColumnGroupReadingStructure readingStructure : readingStructures) {
            if (!readingStructure.isFinished()) {
                return false;
            }
        }
        return true;
    }

}