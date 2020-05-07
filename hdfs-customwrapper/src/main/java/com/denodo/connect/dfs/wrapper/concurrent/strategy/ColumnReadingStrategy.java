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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.MessageTypeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.dfs.commons.naming.Parameter;
import com.denodo.connect.dfs.reader.ParquetSchemaHolder;
import com.denodo.connect.dfs.util.io.PathIterator;
import com.denodo.connect.dfs.util.schema.VDPSchemaUtils;
import com.denodo.connect.dfs.wrapper.concurrent.ColumnGroupReadingStructure;
import com.denodo.connect.dfs.wrapper.concurrent.ColumnsReaderTask;
import com.denodo.connect.dfs.wrapper.concurrent.ReaderManager;
import com.denodo.connect.dfs.wrapper.concurrent.RecordsAssemblerTask;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class ColumnReadingStrategy implements ReadingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnReadingStrategy.class);

    public static final int READING_CHUNK_SIZE = 20000;
    private static final int READING_CHUNK_QUEUE_SIZE = 3;

    private final PathIterator pathIterator;
    private final Configuration conf;
    private final MessageType parquetSchema;
    private final List<CustomWrapperFieldExpression> projectedFields;
    private final Collection<String> conditionFields;
    private final Filter filter;
    private final CustomWrapperResult result;
    private final int parallelismLevel;
    private final ReaderManager readerManager;
    private final AtomicBoolean stopRequested;


    public ColumnReadingStrategy(final PathIterator pathIterator, final Configuration conf,
        final ParquetSchemaHolder schemaHolder, final List<CustomWrapperFieldExpression> projectedFields,
        final Filter filter, final CustomWrapperResult result, final int parallelismLevel,
        final ReaderManager readerManager, final AtomicBoolean stopRequested) {

        this.pathIterator = pathIterator;
        this.conf = conf;
        this.parquetSchema = schemaHolder.getFileSchema();
        this.projectedFields = projectedFields;
        this.conditionFields = schemaHolder.getConditionFields();
        this.filter = filter;
        this.result = result;
        this.parallelismLevel = parallelismLevel;
        this.readerManager = readerManager;
        this.stopRequested = stopRequested;
    }

    @Override
    public void read() throws ExecutionException {

        final long start = System.currentTimeMillis();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Reading by column with parallelism level " + this.parallelismLevel);
        }

        final int readersParallelism = this.parallelismLevel - 1; // the one we substract here is for the RecordsAssembleTask
        final Collection<Callable> readers = new ArrayList<>(readersParallelism);

        final List<List<CustomWrapperFieldExpression>> splitProjections = split(this.projectedFields, readersParallelism);
        final List<List<Type>> splitSchemas = getSplitSchemas(this.parquetSchema, splitProjections);
        addConditions(splitSchemas, this.conditionFields, this.parquetSchema);
        final List<MessageType> schemasWithConditions = buildSchemas(this.parquetSchema.getName(), splitSchemas);

        final List<ColumnGroupReadingStructure> readingStructures = new ArrayList<>(schemasWithConditions.size());
        for (int i = 0; i < schemasWithConditions.size(); i++) {
            readingStructures.add(new ColumnGroupReadingStructure(READING_CHUNK_SIZE, READING_CHUNK_QUEUE_SIZE));
        }


        while (this.pathIterator.hasNext() && !this.stopRequested.get()) {
            final Path currentPath = this.pathIterator.next();
            final Iterator<MessageType> schemasWithConditionsIt = schemasWithConditions.iterator();

            int i = 0;
            final int [] columnOffsets = new int[schemasWithConditions.size()];
            columnOffsets[i] = 0;
            while (schemasWithConditionsIt.hasNext() && !this.stopRequested.get()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Reader task: " + i);
                }

                final List<CustomWrapperFieldExpression> splitProjection = splitProjections.get(i);
                if ((i + 1) < columnOffsets.length) {
                    columnOffsets[i + 1] = columnOffsets[i] + splitProjection.size();
                }

                final ColumnGroupReadingStructure readingStructure = readingStructures.get(i);
                readingStructure.reset();

                readers.add(new ColumnsReaderTask(i, this.conf, currentPath, schemasWithConditionsIt.next(),
                    VDPSchemaUtils.isProjected(Parameter.FULL_PATH, splitProjection), this.filter, splitProjection,
                    readingStructure, this.stopRequested));
                i++;
            }

            readers.add(new RecordsAssemblerTask(this.result, this.projectedFields, readingStructures, columnOffsets,
                this.stopRequested));

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

    private static List<List<Type>> getSplitSchemas(final MessageType fileSchema,
        final List<List<CustomWrapperFieldExpression>> splitProjections) {

        final List<List<Type>> splitSchemas = new ArrayList<>(splitProjections.size());
        for (final List<CustomWrapperFieldExpression> splitProjection : splitProjections) {
            final List<Type> splitSchema = new ArrayList<>(splitProjection.size());
            for (final CustomWrapperFieldExpression projectedField : splitProjection) {
                if (fileSchema.containsField(projectedField.getName())) {
                    splitSchema.add(fileSchema.getType(projectedField.getName()));
                }
            }

            splitSchemas.add(splitSchema);
        }

        return splitSchemas;
    }

    private static <E> List<List<E>> split(final List<? extends E> objects, final int parallelism) {

        final int totalObjects = objects.size();
        int totalGroups = parallelism;
        if (totalObjects < parallelism) {
            totalGroups = totalObjects;
        }
        final int div = totalObjects / totalGroups;
        final int mod = totalObjects % totalGroups;

        final List<List<E>> groups = new ArrayList<>(totalGroups);
        for (int i = 0; i < totalGroups; i++) {
            groups.add(new ArrayList<>(objects.subList(i * div + min(i, mod), (i + 1) * div + min(i + 1, mod))));
        }

        return groups;

    }

    /*
     * Condition fields are added to each columnGroup that is going to be read, so the proper filtering is done
     * when reading column values from a parquet file.
     */
    private static void addConditions(final List<List<Type>> columnGroups, final Collection<String> conditionFields,
        final MessageType parquetSchema) {

        for (final List<Type> columnGroup: columnGroups) {
            for (final String conditionField : conditionFields) {
                final Type conditionType = parquetSchema.getType(conditionField);
                if (! columnGroup.contains(conditionType)) {
                    columnGroup.add(conditionType);
                }
            }
        }
    }

    private static List<MessageType> buildSchemas(final String schemaName, final List<List<Type>> columnGroups) {

        final List<MessageType> schemas = new ArrayList<>(columnGroups.size());

        for (final List<Type> columnGroup : columnGroups) {
            final MessageTypeBuilder schemaBuilder = org.apache.parquet.schema.Types.buildMessage();
            for (final Type column : columnGroup) {
                schemaBuilder.addField(column);
            }

            final MessageType schema = schemaBuilder.named(schemaName);
            schemas.add(schema);
        }

        return schemas;
    }

}