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


import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.AUTOMATIC_PARALLELISM;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.CLUSTER_PARTITION_FIELDS;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.COLUMN_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.FILESYSTEM_URI;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.FILE_NAME_PATTERN;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.FILE_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.INCLUDE_PATH_COLUMN;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.NOT_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.PARALLELISM_LEVEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.PARALLELISM_TYPE;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.PARQUET_FILE_PATH;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.ROW_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.THREADPOOL_SIZE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_EQ;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_NE;

import java.io.IOException;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.ParquetSchemaHolder;
import com.denodo.connect.hadoop.hdfs.util.io.ConditionUtils;
import com.denodo.connect.hadoop.hdfs.util.io.PathIterator;
import com.denodo.connect.hadoop.hdfs.util.schema.VDPSchemaUtils;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ReaderManagerFactory;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.strategy.AutomaticReadingStrategy;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.strategy.ColumnReadingStrategy;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.strategy.FileReadingStrategy;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.strategy.NonConcurrentReadingStrategy;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.strategy.ReadingStrategy;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.strategy.RowGroupReadingStrategy;
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


    private static final int DEFAULT_PARALLELISM = computeParallelism();
    private static final int DEFAULT_POOL_SIZE = DEFAULT_PARALLELISM * 2;

    private final AtomicBoolean stopRequested = new AtomicBoolean(false);

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(PARQUET_FILE_PATH,
                "Parquet File Path",
                true, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(FILE_NAME_PATTERN,
                "Regular expression to filter file names. Example: (.*)\\.parquet ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INCLUDE_PATH_COLUMN,
                "Include path column? ",
                false, true, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(PARALLELISM_TYPE,
                "Type of parallelism, if any ",
                false, true, CustomWrapperInputParameterTypeFactory.enumStringType(
                    new String[] {NOT_PARALLEL, AUTOMATIC_PARALLELISM, FILE_PARALLEL, ROW_PARALLEL, COLUMN_PARALLEL})),
            new CustomWrapperInputParameter(PARALLELISM_LEVEL,
                "Level of parallelism ",
                false, true, CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(CLUSTER_PARTITION_FIELDS,
                "Files sorted by this field/s (separated by commas) ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType())
    };

    private static final CustomWrapperInputParameter[] DATA_SOURCE_INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(FILESYSTEM_URI,
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
            new CustomWrapperInputParameter(THREADPOOL_SIZE,
                "Number of threads in the pool",
                false, true, CustomWrapperInputParameterTypeFactory.integerType())
    };

    private static int computeParallelism() {

        final int processors = Runtime.getRuntime().availableProcessors();
        return processors > 1 ? processors -1 : processors;
    }

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperInputParameter[] getDataSourceInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(DATA_SOURCE_INPUT_PARAMETERS,
            super.getDataSourceInputParameters());

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
            final Path path = new Path(inputValues.get(PARQUET_FILE_PATH));
            final String fileNamePattern = inputValues.get(FILE_NAME_PATTERN);

            pathIterator = new PathIterator(conf, path, fileNamePattern, null);
            final ParquetSchemaHolder schemaHolder = new ParquetSchemaHolder(conf, pathIterator.next(), null, null);
            final SchemaElement javaSchema = schemaHolder.getWrapperSchema();
            final CustomWrapperSchemaParameter[] schemaParameters = VDPSchemaUtils.buildSchemaParameterParquet(javaSchema.getElements());

            final String clusteringFields = inputValues.get(CLUSTER_PARTITION_FIELDS);
            if (clusteringFields != null && !VDPSchemaUtils.isSchemaField(clusteringFields, schemaParameters)) {
                throw new IllegalArgumentException('\'' + clusteringFields + "' is/are not valid for the schema: "
                    + VDPSchemaUtils.toString(schemaParameters));
            }

            final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(INCLUDE_PATH_COLUMN));
            if (includePathColumn) {
                final CustomWrapperSchemaParameter filePath = new CustomWrapperSchemaParameter(Parameter.FULL_PATH,
                    Types.VARCHAR, null, false, CustomWrapperSchemaParameter.NOT_SORTABLE,
                    false, true, false);
                return (CustomWrapperSchemaParameter[]) ArrayUtils.add(schemaParameters, filePath);
            } else {
                return schemaParameters;
            }

        } catch (final NoSuchElementException e) {
            throw new CustomWrapperException("There are no files in " + inputValues.get(PARQUET_FILE_PATH)
            + (StringUtils.isNotBlank(inputValues.get(FILE_NAME_PATTERN))
                ? " matching the provided file pattern: " + inputValues.get(FILE_NAME_PATTERN) : ""));
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

        this.stopRequested.set(false);

        validateConcurrentConfiguration(inputValues);


        final Configuration conf = getHadoopConfiguration(inputValues);
        final Path path = new Path(inputValues.get(PARQUET_FILE_PATH));
        final String fileNamePattern = inputValues.get(FILE_NAME_PATTERN);
        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(INCLUDE_PATH_COLUMN))
            && VDPSchemaUtils.isProjected(Parameter.FULL_PATH, projectedFields);
        final String parallelismType = (inputValues.get(PARALLELISM_TYPE) != null) ? inputValues.get(PARALLELISM_TYPE): NOT_PARALLEL;
        final int parallelismLevel = inputValues.get(PARALLELISM_LEVEL) == null ? DEFAULT_PARALLELISM
            : Integer.parseInt(inputValues.get(PARALLELISM_LEVEL));
        final String fileSystemURI = inputValues.get(FILESYSTEM_URI);
        final int threadPoolSize = inputValues.get(THREADPOOL_SIZE) == null ? DEFAULT_POOL_SIZE
            : Integer.parseInt(inputValues.get(THREADPOOL_SIZE));

        PathIterator pathIterator = null;
        try {

            pathIterator = new PathIterator(conf, path, fileNamePattern, null);
            final ParquetSchemaHolder schemaHolder = new ParquetSchemaHolder(conf, pathIterator.next(), projectedFields, condition);
            final CustomWrapperConditionHolder fixedCondition = fixWrapperConditionWithPartitionInfo(pathIterator, condition, schemaHolder);

            final Filter filter = getFilter(fixedCondition, schemaHolder);
            pathIterator.reset();

            ReadingStrategy readingStrategy = null;
            switch (parallelismType) {
                case AUTOMATIC_PARALLELISM:
                    final String clusteringFields = inputValues.get(CLUSTER_PARTITION_FIELDS);
                    readingStrategy = new AutomaticReadingStrategy(pathIterator, conf, schemaHolder, projectedFields,
                        filter, includePathColumn, result, parallelismLevel, fileSystemURI, threadPoolSize,
                        clusteringFields, fixedCondition.getComplexCondition(), this.stopRequested);

                    break;
                case ROW_PARALLEL:
                    readingStrategy = new RowGroupReadingStrategy(pathIterator, conf, schemaHolder, projectedFields,
                        filter, includePathColumn, result, parallelismLevel, ReaderManagerFactory.get(fileSystemURI, threadPoolSize),
                        this.stopRequested);

                    break;
                case FILE_PARALLEL:
                    readingStrategy = new FileReadingStrategy(pathIterator, conf, schemaHolder, projectedFields, filter,
                        includePathColumn, result, parallelismLevel, ReaderManagerFactory.get(fileSystemURI, threadPoolSize),
                        this.stopRequested);

                    break;
                case COLUMN_PARALLEL:
                    readingStrategy = new ColumnReadingStrategy(pathIterator, conf, schemaHolder, projectedFields,
                        filter, result, parallelismLevel, ReaderManagerFactory.get(fileSystemURI, threadPoolSize),
                        this.stopRequested);

                    break;
                default:
                    readingStrategy = new NonConcurrentReadingStrategy(pathIterator, conf, schemaHolder, projectedFields,
                        filter, includePathColumn, result, this.stopRequested);

                    break;

            }

            readingStrategy.read();

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

    @Override
    public boolean stop() {
        this.stopRequested.set(true);
        return super.stop();
    }

    private static void validateConcurrentConfiguration(final Map<String, String> inputValues) {

        final String parallelismType = (inputValues.get(PARALLELISM_TYPE) != null) ? inputValues.get(PARALLELISM_TYPE): NOT_PARALLEL;
        if (! NOT_PARALLEL.equals(parallelismType)) {
            final int threadPoolSize = inputValues.get(THREADPOOL_SIZE) == null ? DEFAULT_POOL_SIZE
                : Integer.parseInt(inputValues.get(THREADPOOL_SIZE));
            final int parallelism = inputValues.get(PARALLELISM_LEVEL) == null ? DEFAULT_PARALLELISM
                : Integer.parseInt(inputValues.get(PARALLELISM_LEVEL));

            if (parallelism > threadPoolSize) {
                throw new IllegalArgumentException(PARALLELISM_LEVEL + " (" + parallelism + ") cannot be less than "
                    + THREADPOOL_SIZE + " (" + threadPoolSize + ')');
            }

            final int minimumParallelismLevel = COLUMN_PARALLEL.equals(parallelismType) ? 3 : 2;
            if (parallelism < minimumParallelismLevel) {
                throw new IllegalArgumentException(minimumParallelismLevel
                    + " is the minimum level of parallelism that is accepted for the read option selected");
            }
        }
    }

    /*
     * If some of the VDP conditions are related to the partitioning conditions of the dataset:
     *  - these conditions will be given to the PathIterator for pruning the partitions.
     *  - these conditions will be removed from the CustomWrapperCondition, as they cannot be applied to Parquet files,
     *        only to the directory names of the partition
     */
    private static CustomWrapperConditionHolder fixWrapperConditionWithPartitionInfo(final PathIterator pathIterator,
        final CustomWrapperConditionHolder condition, final ParquetSchemaHolder schemaHolder) {

        final List<String> partitionFields = schemaHolder.getPartitionFields();
        final Collection<String> conditionFields = schemaHolder.getConditionFields();

        if (Collections.disjoint(partitionFields, conditionFields)) {
            return condition;
        }

        pathIterator.addPartitionConditionsInfo(partitionFields, conditionFields, condition);
        final CustomWrapperConditionHolder fixedCondition = ConditionUtils.removeConditions(condition, partitionFields);
        schemaHolder.updateCondition(fixedCondition);

        return fixedCondition;

    }

    private static Filter getFilter(final CustomWrapperConditionHolder condition, final ParquetSchemaHolder schemaHolder) {

        Filter filter = null;

        final FilterPredicate filterPredicate = ConditionUtils.buildFilter(condition.getComplexCondition(), schemaHolder.getFileSchema());
        if (filterPredicate != null) {
            filter = FilterCompat.get(filterPredicate);
        }

        return filter;
    }
}
