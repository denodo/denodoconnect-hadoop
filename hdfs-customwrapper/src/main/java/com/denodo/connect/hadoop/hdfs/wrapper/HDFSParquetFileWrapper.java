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


import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_EQ;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_NE;

import java.io.IOException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.RowGroupReaderTask;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSParquetFileReader;
import com.denodo.connect.hadoop.hdfs.util.io.PathIterator;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaBuilder;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaUtils;
import com.denodo.connect.hadoop.hdfs.util.schema.VDPSchemaUtils;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ReaderManager;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ReaderTask;
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
 * HDFS file custom wrapper for reading Parquet files stored in HDFS (Hadoop
 * Distributed File System).
 *
 */
public class HDFSParquetFileWrapper extends AbstractSecureHadoopWrapper {

    private static final  Logger LOG = LoggerFactory.getLogger(HDFSParquetFileWrapper.class);


    private static final String FILE_PARALLEL = "File Parallel";
    private static final String ROW_PARALLEL = "Row Parallel";
    private static final String NOT_PARALLEL = "Not parallel";
    private static final String NUM_FILES_PARALLEL = "NUM_FILES_PARALLEL";
    private static final String INVOKE_ADDROW = "INVOKE_ADDROW";

    private long count;

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
                true, true, CustomWrapperInputParameterTypeFactory.enumStringType(new String[]{ NOT_PARALLEL, FILE_PARALLEL, ROW_PARALLEL})),
            new CustomWrapperInputParameter(NUM_FILES_PARALLEL,
                "Num of files in parallel ",
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
                false, true, CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP})),
            new CustomWrapperInputParameter(Parameter.HDFS_SITE_PATH,
                "Local route of hdfs-site.xml configuration file ",
                false, true, CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP}))
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
            OPERATOR_EQ, OPERATOR_NE, OPERATOR_LT, OPERATOR_LE,
            OPERATOR_GT, OPERATOR_GE
        });
        return configuration;
    }

    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(final Map<String, String> inputValues)
        throws CustomWrapperException {

        PathIterator pathIterator  = null;
        try {

            final Configuration conf = getHadoopConfiguration(inputValues);
            final Path path = new Path(inputValues.get(Parameter.PARQUET_FILE_PATH));
            final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);
            final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

            pathIterator = new PathIterator(conf, path, fileNamePattern, null);
            final ParquetSchemaBuilder schemaBuilder = new ParquetSchemaBuilder(conf, pathIterator.next(), null, null);

            final SchemaElement javaSchema = schemaBuilder.getSchema();
            if (includePathColumn){
                final CustomWrapperSchemaParameter filePath = new CustomWrapperSchemaParameter(Parameter.FULL_PATH, Types.VARCHAR, null, false,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false);
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

    @Override
    public void doRun(final CustomWrapperConditionHolder condition, final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperResult result, final Map<String, String> inputValues) throws CustomWrapperException {

        final Configuration conf = getHadoopConfiguration(inputValues);
        final Path path = new Path(inputValues.get(Parameter.PARQUET_FILE_PATH));
        final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);
        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));
        final String readOptions = inputValues.get(Parameter.READ_OPTIONS);
        final int parallelism = Integer.parseInt(inputValues.get(NUM_FILES_PARALLEL));
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
                        schemaBuilder.getRowGroups(), schemaBuilder.getFooter());
                    break;
                case FILE_PARALLEL:
                    parallelRead(pathIterator, conf, schemaBuilder.getProjectedSchema(), projectedFields,
                        schemaBuilder.getConditionFields(), filter, includePathColumn, result, parallelism, invokeAddRow);
                    break;
                case NOT_PARALLEL:
                    singleRead(pathIterator, conf, schemaBuilder.getProjectedSchema(), projectedFields,
                        schemaBuilder.getConditionFields(), filter, includePathColumn, result, invokeAddRow);
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

    private void parallelReadByRowGroup(final PathIterator pathIterator, final Configuration conf, final MessageType schema,
        final List<CustomWrapperFieldExpression> projectedFields, final List<String> conditionFields, final Filter filter,
        final boolean includePathColumn, final CustomWrapperResult result, final int parallelism, final boolean invokeAddRow,
        final List<BlockMetaData> rowGroups, final ParquetMetadata parquetMetadata) throws ExecutionException,
        IOException {

        final ReaderManager readerManager = ReaderManager.getInstance();

        final Collection<Callable> readers = new ArrayList<>(parallelism);
        int i = 0;
        List<BlockMetaData> fileRowGroups = null;
        while (pathIterator.hasNext() && ! isStopRequested()) {

            final Path currentPath = pathIterator.next();

            //We reuse rowGroups value initialized in the schema in the first file of the path
            fileRowGroups = (fileRowGroups == null) ? rowGroups : ParquetSchemaUtils.getParquetRowGroups(conf, currentPath);
            final List<List<BlockMetaData>> rowGroupsList = this.generateRowGroupsList(fileRowGroups, parallelism);

            final Iterator<List<BlockMetaData>> rowGroupListIterator = rowGroupsList.iterator();
            while (rowGroupListIterator.hasNext() && ! isStopRequested()) {
                while (rowGroupListIterator.hasNext() && i < parallelism && ! isStopRequested()) {
                    readers.add(new RowGroupReaderTask(conf, currentPath, schema, includePathColumn,  conditionFields,
                        filter, projectedFields, result, rowGroupListIterator.next(), invokeAddRow, parquetMetadata));
                    i++;
                }
                if (! (isStopRequested()) && ! (i < parallelism)) {
                    readerManager.execute(readers);
                    readers.clear();
                    i = 0;
                }
            }
        }
        if (! isStopRequested()) {
            readerManager.execute(readers);
            readers.clear();
        }
    }

    private List<List<BlockMetaData>> generateRowGroupsList(List<BlockMetaData> rowGroups, final int parallelism) {
        List<List<BlockMetaData>> rowGroupsList = new ArrayList<>();

        int numberOfRowGroups = rowGroups.size();

        int numberOfRowGroupsByRowGroupList =  numberOfRowGroups > parallelism ? numberOfRowGroups / parallelism : 1;
        int numberOfRowGroupsByRowGroupListWithOffset = numberOfRowGroups % parallelism == 0 || numberOfRowGroups <= parallelism ? numberOfRowGroupsByRowGroupList : numberOfRowGroupsByRowGroupList + 1;

        final AtomicInteger counter = new AtomicInteger();

        for (BlockMetaData rowGroup : rowGroups) {
            if (counter.getAndIncrement() % numberOfRowGroupsByRowGroupListWithOffset == 0) {
                rowGroupsList.add(new ArrayList<>());
            }
            rowGroupsList.get(rowGroupsList.size() - 1).add(rowGroup);
        }
        return rowGroupsList;
    }

    private void parallelRead(final PathIterator pathIterator, final Configuration conf, final MessageType schema,
        final List<CustomWrapperFieldExpression> projectedFields, final List<String> conditionFields, final Filter filter,
        final boolean includePathColumn, final CustomWrapperResult result, final int parallelism, final boolean invokeAddRow)
        throws ExecutionException {

        final ReaderManager readerManager = ReaderManager.getInstance();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Parallelism Level " + parallelism);
        }

        final Collection<Callable> readers = new ArrayList<>(parallelism);
        while (pathIterator.hasNext()&& ! isStopRequested()) {

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

            if (LOG.isTraceEnabled()) {
                if (! invokeAddRow) {
                    LOG.trace("TUPLES " + this.count);
                }
            }
        }
    }
}
