/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2013, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.hdfs.wrapper;


import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.COLUMN_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.FILE_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.NOT_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.PARALLELISM_LEVEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.ROW_PARALLEL;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_EQ;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_NE;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.MessageTypeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSParquetFileReader;
import com.denodo.connect.hadoop.hdfs.util.io.PathIterator;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaBuilder;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaUtils;
import com.denodo.connect.hadoop.hdfs.util.schema.VDPSchemaUtils;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ColumnGroupReadingStructure;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ColumnsReaderTask;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ReaderManager;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ReaderManagerFactory;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ReaderTask;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.RecordsAssemblerTask;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.RowGroupReaderTask;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;

/**
 * HDFS file custom wrapper for reading Parquet files stored in HDFS (Hadoop Distributed File System).
 *
 */
public class HDFSParquetFileWrapper extends AbstractSecureHadoopWrapper {

    private static final  Logger LOG = LoggerFactory.getLogger(HDFSParquetFileWrapper.class);

    public static final int READING_CHUNK_SIZE = 20000;
    private static final int READING_CHUNK_QUEUE_SIZE = 3;

    private static final String INVOKE_ADDROW = "INVOKE_ADDROW";

    private long count;
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.PARQUET_FILE_PATH,
                "Parquet File Path",
                true, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.FILE_NAME_PATTERN,
                "Regular expression to filter file names. Example: (.*)\\.parquet ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.INCLUDE_PATH_COLUMN,
                "Include path column? ",
                false, true, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.READ_OPTIONS,
                "Read options ",
                true, true, CustomWrapperInputParameterTypeFactory.enumStringType(
                    new String[] {NOT_PARALLEL, FILE_PARALLEL, ROW_PARALLEL, COLUMN_PARALLEL})),
            new CustomWrapperInputParameter(PARALLELISM_LEVEL,
                "Level of parallelism ",
                false, true, CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(INVOKE_ADDROW,
                "Invoke addRow? ",
                false, true, CustomWrapperInputParameterTypeFactory.booleanType(false))
    };

    private static final CustomWrapperInputParameter[] DATA_SOURCE_INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                "e.g. hdfs://<ip>:<port> or s3a://<bucket> ",
                true, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.CORE_SITE_PATH,
                "Local route of core-site.xml configuration file ",
                false, true, CustomWrapperInputParameterTypeFactory.routeType(
                    new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP})),
            new CustomWrapperInputParameter(Parameter.HDFS_SITE_PATH,
                "Local route of hdfs-site.xml configuration file ",
                false, true, CustomWrapperInputParameterTypeFactory.routeType(
                    new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP})),
            new CustomWrapperInputParameter(Parameter.THREADPOOL_SIZE,
                "Number of threads in the pool (default is 20)",
                false, true, CustomWrapperInputParameterTypeFactory.integerType())
    };

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperInputParameter[] getDataSourceInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(DATA_SOURCE_INPUT_PARAMETERS, super.getDataSourceInputParameters());

    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        final CustomWrapperConfiguration configuration = super.getConfiguration();
        configuration.setDelegateProjections(true);
        configuration.setDelegateOrConditions(true);
        configuration.setAllowedOperators(new String[] {
            OPERATOR_EQ, OPERATOR_NE, OPERATOR_LT, OPERATOR_LE, OPERATOR_GT, OPERATOR_GE
        });
        return configuration;
    }

    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(final Map<String, String> inputValues)
            throws CustomWrapperException {

        PathIterator pathIterator  = null;
        try {

            validateConcurrentConfiguration(inputValues);
            
            final Configuration conf = getHadoopConfiguration(inputValues);
            final Path path = new Path(inputValues.get(Parameter.PARQUET_FILE_PATH));
            final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);
            final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

            pathIterator = new PathIterator(conf, path, fileNamePattern, null);
            final ParquetSchemaBuilder schemaBuilder = new ParquetSchemaBuilder(conf, pathIterator.next(), null, null);

            final SchemaElement javaSchema = schemaBuilder.getSchema();
            if (includePathColumn) {
                final CustomWrapperSchemaParameter filePath = new CustomWrapperSchemaParameter(Parameter.FULL_PATH,
                    Types.VARCHAR, null, false, CustomWrapperSchemaParameter.NOT_SORTABLE,
                    false, true, false);
                return (CustomWrapperSchemaParameter[]) ArrayUtils.add(VDPSchemaUtils.buildSchemaParameterParquet(javaSchema.getElements()), filePath);
            } else {
                return VDPSchemaUtils.buildSchemaParameterParquet(javaSchema.getElements());
            }

        } catch (final NoSuchElementException e) {
            throw new CustomWrapperException("There are no files in " + inputValues.get(Parameter.PARQUET_FILE_PATH) 
            + (StringUtils.isNotBlank(inputValues.get(Parameter.FILE_NAME_PATTERN)) 
                ? " matching the provided file pattern: " + inputValues.get(Parameter.FILE_NAME_PATTERN)
                : ""));
        } catch (final Exception e) {
            LOG.error("Error building wrapper schema", e);
            throw new CustomWrapperException(e.getMessage(), e);
        } finally {
            try {
                if (pathIterator != null ) {
                    pathIterator.close();
                }
            } catch (final IOException e) {
                LOG.error("Error releasing the reader", e);
            }

        }

    }

    private void validateConcurrentConfiguration(final Map<String, String> inputValues) {

        if (! NOT_PARALLEL.equals(inputValues.get(Parameter.READ_OPTIONS))) {
            final int threadPoolSize = inputValues.get(Parameter.THREADPOOL_SIZE) == null ? 0 : Integer.parseInt(inputValues.get(Parameter.THREADPOOL_SIZE));
            final int parallelism = inputValues.get(PARALLELISM_LEVEL) == null ? 0 : Integer.parseInt(inputValues.get(PARALLELISM_LEVEL));

            if (parallelism > threadPoolSize) {
                throw new IllegalArgumentException(PARALLELISM_LEVEL + " (" + parallelism + ") cannot be less than "
                    + Parameter.THREADPOOL_SIZE + " (" + threadPoolSize + ')');
            }

            final int minimumParalellismLevel = COLUMN_PARALLEL.equals(inputValues.get(Parameter.READ_OPTIONS)) ? 3 : 2;
            if (parallelism < minimumParalellismLevel) {
                throw new IllegalArgumentException(minimumParalellismLevel
                    + " is the minimum level of parallelism that is accepted for the read option selected");
            }
        }
    }

    @Override
    public void doRun(final CustomWrapperConditionHolder condition, final List<CustomWrapperFieldExpression> projectedFields,
            final CustomWrapperResult result, final Map<String, String> inputValues) throws CustomWrapperException {

        this.stopRequested.set(false);

        validateConcurrentConfiguration(inputValues);

        final Configuration conf = getHadoopConfiguration(inputValues);
        final Path path = new Path(inputValues.get(Parameter.PARQUET_FILE_PATH));
        final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);
        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN)) && isProjected(Parameter.FULL_PATH, projectedFields);
        final String readOptions = inputValues.get(Parameter.READ_OPTIONS);
        final int parallelism = inputValues.get(PARALLELISM_LEVEL) == null ? 0 : Integer.parseInt(inputValues.get(PARALLELISM_LEVEL));
        final String fileSystemURI = inputValues.get(Parameter.FILESYSTEM_URI);
        final int threadPoolSize = inputValues.get(Parameter.THREADPOOL_SIZE) == null ? 0 : Integer.parseInt(inputValues.get(Parameter.THREADPOOL_SIZE));

        final boolean invokeAddRow = Boolean.parseBoolean(inputValues.get(INVOKE_ADDROW));

        PathIterator pathIterator = null;
        try {

            pathIterator = new PathIterator(conf, path, fileNamePattern, null);
            final ParquetSchemaBuilder schemaBuilder = new ParquetSchemaBuilder(conf, pathIterator.next(), projectedFields, condition);
            SchemaElement schema = null;
            if (schemaBuilder.hasNullValueInConditions()) {
                schema = schemaBuilder.getSchema();
            }

            final FilterPredicate filterPredicate = ParquetSchemaUtils.buildFilter(condition.getComplexCondition(), schema);
            FilterCompat.Filter filter = null;
            if (filterPredicate != null) {
                filter = FilterCompat.get(filterPredicate);
            }

            pathIterator.reset();
            switch (readOptions) {
                case ROW_PARALLEL:
                    parallelReadByRowGroup(pathIterator, conf, schemaBuilder.getProjectedSchema(), projectedFields,
                        schemaBuilder.getConditionFields(), filter, includePathColumn, result, parallelism, invokeAddRow,
                        schemaBuilder.getRowGroups(), schemaBuilder.getFooter(), ReaderManagerFactory.get(fileSystemURI, threadPoolSize));
                    break;
                case FILE_PARALLEL:
                    parallelRead(pathIterator, conf, schemaBuilder.getProjectedSchema(), projectedFields,
                        schemaBuilder.getConditionFields(), filter, includePathColumn, result, parallelism, invokeAddRow,
                        ReaderManagerFactory.get(fileSystemURI, threadPoolSize));
                    break;
                case COLUMN_PARALLEL:
                    parallelReadByColumn(pathIterator, conf, schemaBuilder.getFileSchema(), projectedFields,
                        schemaBuilder.getConditionsIncludingProjectedFields(), schemaBuilder.getConditionFields(),
                        filter, includePathColumn, result, parallelism, invokeAddRow, schemaBuilder.getFooter(),
                        ReaderManagerFactory.get(fileSystemURI, threadPoolSize));
                    break;
                default:
                    singleRead(pathIterator, conf, schemaBuilder.getProjectedSchema(), projectedFields,
                        schemaBuilder.getConditionFields(), filter, includePathColumn, result, invokeAddRow);
                    break;

            }

        } catch (final Exception e) {
            LOG.error("Error accessing Parquet file", e);
            throw new CustomWrapperException("Error accessing Parquet file: " + e.getMessage(), e);

        } finally {
            try {
                if (pathIterator != null ) {
                    pathIterator.close();
                }
            } catch (final IOException e) {
                LOG.error("Error releasing the reader", e);
            }
        }
    }

    private boolean isProjected(final String field, final List<CustomWrapperFieldExpression> projectedFields) {

        for (final CustomWrapperFieldExpression projectedField : projectedFields) {
            if (field.equals(projectedField.getName())) {
                return true;
            }
        }

        return false;
    }

    private void parallelReadByRowGroup(final PathIterator pathIterator, final Configuration conf, final MessageType schema,
        final List<CustomWrapperFieldExpression> projectedFields, final List<String> conditionFields, final Filter filter,
        final boolean includePathColumn, final CustomWrapperResult result, final int parallelism, final boolean invokeAddRow,
        final List<BlockMetaData> rowGroups, final ParquetMetadata parquetMetadata, final ReaderManager readerManager) throws ExecutionException,
        IOException {

        final Collection<Callable> readers = new ArrayList<>(parallelism);
        List<BlockMetaData> fileRowGroups = null;
        while (pathIterator.hasNext() && ! isStopRequested()) {

            final Path currentPath = pathIterator.next();

            //We reuse rowGroups value initialized in the schema in the first file of the path
            fileRowGroups = (fileRowGroups == null) ? rowGroups : ParquetSchemaUtils.getRowGroups(conf, currentPath);
            final List<List<BlockMetaData>> rowGroupsList = generateRowGroupsList(fileRowGroups, parallelism);

            final Iterator<List<BlockMetaData>> rowGroupListIterator = rowGroupsList.iterator();
            while (rowGroupListIterator.hasNext() && ! isStopRequested()) {
                readers.add(new RowGroupReaderTask(conf, currentPath, schema, includePathColumn,  conditionFields,
                    filter, projectedFields, result, rowGroupListIterator.next(), invokeAddRow, parquetMetadata));
            }

            if (! (isStopRequested())) {
                readerManager.execute(readers);
                readers.clear();
            }
        }
    }

    private List<List<BlockMetaData>> generateRowGroupsList(final List<BlockMetaData> rowGroups, final int parallelism) {

        final List<List<BlockMetaData>> rowGroupsList = new ArrayList<>(parallelism);

        final int numberOfRowGroups = rowGroups.size();
        final int sizeRowGroups = numberOfRowGroups % parallelism == 0
            ? (numberOfRowGroups / parallelism) : (numberOfRowGroups / parallelism) + 1;

        final AtomicInteger counter = new AtomicInteger();
        for (final BlockMetaData rowGroup : rowGroups) {
            if (counter.getAndIncrement() % sizeRowGroups == 0) {
                rowGroupsList.add(new ArrayList<>(sizeRowGroups));
            }
            rowGroupsList.get(rowGroupsList.size() - 1).add(rowGroup);
        }
        return rowGroupsList;
    }

    private void parallelReadByColumn(final PathIterator pathIterator, final Configuration conf, final MessageType fileSchema,
        final List<CustomWrapperFieldExpression> projectedFields, final List<String> conditionIncludingProjectedFields,
        final List<String> conditionFields, final Filter filter, final boolean includePathColumn,
        final CustomWrapperResult result, final int parallelism, final boolean invokeAddRow,
        final ParquetMetadata parquetMetadata, final ReaderManager readerManager) throws ExecutionException {

        final long start = System.currentTimeMillis();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Parallelism Level " + parallelism);
        }

        final int readersParallelism = parallelism - 1; // the one we substract here is for the RecordsAssembleTask
        final Collection<Callable> readers = new ArrayList<>(readersParallelism);

        // TODO: extract this logic to the proper class
        final MessageType projectedSchema = buildProjectedSchema(fileSchema, projectedFields);
        final List<List<Type>> columnGroups = splitSchema(projectedSchema, readersParallelism);
        final List<MessageType> schemasWithoutConditions = buildSchemas(projectedSchema.getName(), columnGroups);
        addConditions(columnGroups, conditionIncludingProjectedFields, fileSchema);
        final List<MessageType> schemas = buildSchemas(projectedSchema.getName(), columnGroups);
        // TODO end

        final List<ColumnGroupReadingStructure> readingStructures = new ArrayList<>(schemas.size());
        for (int i = 0; i < schemas.size(); i++) {
            readingStructures.add(new ColumnGroupReadingStructure(READING_CHUNK_SIZE, READING_CHUNK_QUEUE_SIZE));
        }

        while (pathIterator.hasNext() && ! isStopRequested()) {
            final Path currentPath = pathIterator.next();
            final Iterator<MessageType> schemasIterator = schemas.iterator();

            int i = 0;
            final int [] columnOffsets = new int[schemasWithoutConditions.size()];
            columnOffsets[i] = 0;
            while (schemasIterator.hasNext() && !isStopRequested()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Reader task: " + i);
                }

                final MessageType resultsSchema = schemasWithoutConditions.get(i);
                if ((i + 1) < columnOffsets.length) {
                    columnOffsets[i + 1] = columnOffsets[i] + resultsSchema.getFieldCount();
                }

                readers.add(new ColumnsReaderTask(i, conf, currentPath, schemasIterator.next(),
                    resultsSchema, conditionFields, filter, readingStructures.get(i), parquetMetadata, this.stopRequested));
                i++;
            }

            readers.add(new RecordsAssemblerTask(result, projectedFields, readingStructures, columnOffsets,
                includePathColumn ? currentPath.toString() : null, invokeAddRow, this.stopRequested));

            if (!isStopRequested()) {
                readerManager.execute(readers);
                readers.clear();
            }
        }

        final long end = System.currentTimeMillis();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Elapsed time " + (end - start));
        }
    }

    private MessageType buildProjectedSchema(final MessageType fileSchema, final List<CustomWrapperFieldExpression> projectedFields) {

        final MessageTypeBuilder schemaBuilder = org.apache.parquet.schema.Types.buildMessage();
        for (final CustomWrapperFieldExpression projectedField : projectedFields) {
            if (fileSchema.containsField(projectedField.getName())) {
                schemaBuilder.addField(fileSchema.getType(projectedField.getName()));
            }
        }

        return schemaBuilder.named(fileSchema.getName());
    }

    private List<List<Type>> splitSchema(final MessageType schema, final int parallelism) {

        final int columns = schema.getFieldCount();
        final int sizeColumnGroups = columns % parallelism == 0 ? (columns / parallelism) : (columns / parallelism) + 1;

        final List<List<Type>> columnGroups = new ArrayList<>(parallelism);

        final AtomicInteger counter = new AtomicInteger();
        for (final Type type : schema.getFields()) {
            if (counter.getAndIncrement() % sizeColumnGroups == 0) {
                columnGroups.add(new ArrayList<>(sizeColumnGroups));
            }
            columnGroups.get(columnGroups.size() - 1).add(type);
        }

        return columnGroups;

    }

    /*
     * Condition fields are added to each columnGroup that is going to be read, so the proper filtering is done
     * when reading column values from a parquet file.
     */
    private void addConditions(final List<List<Type>> columnGroups, final List<String> conditionFields,
        final MessageType fileSchema) {

        for (final List<Type> columnGroup: columnGroups) {
            for (final String conditionField : conditionFields) {
                final Type conditionType = fileSchema.getType(conditionField);
                if (! columnGroup.contains(conditionType)) {
                    columnGroup.add(conditionType);
                }
            }
        }
    }

    private List<MessageType> buildSchemas(final String schemaName, final List<List<Type>> columnGroups) {

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

    private void parallelRead(final PathIterator pathIterator, final Configuration conf, final MessageType schema,
        final List<CustomWrapperFieldExpression> projectedFields, final List<String> conditionFields, final Filter filter,
        final boolean includePathColumn, final CustomWrapperResult result, final int parallelism, final boolean invokeAddRow,
        final ReaderManager readerManager) throws ExecutionException {

        if (LOG.isTraceEnabled()) {
            LOG.trace("Parallelism Level " + parallelism);
        }

        final Collection<Callable> readers = new ArrayList<>(parallelism);
        while (pathIterator.hasNext() && ! isStopRequested()) {

            int i = 0;
            while (pathIterator.hasNext() && i < parallelism) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Reader task: " + i);
                }
                readers.add(new ReaderTask(conf, pathIterator.next(), schema, includePathColumn,  conditionFields, filter,
                    projectedFields, result, invokeAddRow));
                i++;
            }

            if (! isStopRequested()) {
                readerManager.execute(readers);
                readers.clear();
            }
        }
    }

    private void singleRead(final PathIterator pathIterator, final Configuration conf, final MessageType schema,
        final List<CustomWrapperFieldExpression> projectedFields, final List<String> conditionFields, final Filter filter,
        final boolean includePathColumn, final CustomWrapperResult result, final boolean invokeAddRow) throws IOException {

        final long start = System.currentTimeMillis();
        while (pathIterator.hasNext()) {
            final HDFSParquetFileReader reader = new HDFSParquetFileReader(conf, pathIterator.next(),
                includePathColumn, filter, schema, conditionFields, null, null, null);

            Object parquetData = reader.read();
            while (parquetData != null && ! isStopRequested()) {
                if (invokeAddRow) {
                    result.addRow((Object[]) parquetData, projectedFields);
                }  else {
                    this.count += ((Object[]) parquetData).length;
                }
                parquetData = reader.read();
            }

            final long end = System.currentTimeMillis();

            if (LOG.isTraceEnabled()) {
                LOG.trace("Elapsed time " + (end - start));
                if (! invokeAddRow) {
                    LOG.trace("TUPLES " + this.count);
                }
            }
        }
    }


    @Override
    public boolean stop() {
        this.stopRequested.set(true);
        return super.stop();
    }
}
