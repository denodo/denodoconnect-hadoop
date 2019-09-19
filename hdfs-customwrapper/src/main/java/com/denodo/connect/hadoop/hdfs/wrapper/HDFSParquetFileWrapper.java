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


import java.io.IOException;
import java.sql.Types;
import java.util.*;

import com.denodo.vdb.engine.customwrapper.condition.*;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSParquetFileReader;
import com.denodo.connect.hadoop.hdfs.util.schema.VDPSchemaUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;

import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.*;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

/**
 * HDFS file custom wrapper for reading Parquet files stored in HDFS (Hadoop
 * Distributed File System).
 * <p>
 *
 * The following parameters are required: file system URI, Avro
 * schema file path or Avro schema JSON. <br/>
 *
 */
public class HDFSParquetFileWrapper extends AbstractSecureHadoopWrapper {

    private static final  Logger LOG = LoggerFactory.getLogger(HDFSParquetFileWrapper.class);

    
    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                "e.g. hdfs://<ip>:<port> or s3n://<id>:<secret>\\\\@<bucket>t ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.PARQUET_FILE_PATH,
                "Parquet File Path",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.FILE_NAME_PATTERN,
                    "Regular expression to filter file names. Example: (.*)\\.parquet ", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),               
            new CustomWrapperInputParameter(Parameter.CORE_SITE_PATH,
                "Local route of core-site.xml configuration file ",
                false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP})),
            new CustomWrapperInputParameter(Parameter.HDFS_SITE_PATH,
                "Local route of hdfs-site.xml configuration file ",
                false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP})),
            new CustomWrapperInputParameter(Parameter.INCLUDE_PATH_COLUMN,
                "Include path column? ", false,
                CustomWrapperInputParameterTypeFactory.booleanType(false))
    };

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(INPUT_PARAMETERS, super.getInputParameters());
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

        HDFSParquetFileReader reader = null;
        try {

            final Configuration conf = getHadoopConfiguration(inputValues);

            final String parquetFilePath = inputValues.get(Parameter.PARQUET_FILE_PATH);
            final Path path = new Path(parquetFilePath);
            
            final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);

            final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

            reader = new HDFSParquetFileReader(conf, path, fileNamePattern, null, null, includePathColumn, true,null);

            final SchemaElement javaSchema = reader.getSchema(conf);
            if(includePathColumn){
                final CustomWrapperSchemaParameter filePath = new CustomWrapperSchemaParameter(Parameter.FULL_PATH, Types.VARCHAR, null, true,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false);
                return (CustomWrapperSchemaParameter[]) ArrayUtils.add(VDPSchemaUtils.buildSchemaParameterParquet(javaSchema.getElements()),filePath);
            }else {
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
                if (reader != null ) {
                    reader.close();
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

        final String parquetFilePath = inputValues.get(Parameter.PARQUET_FILE_PATH);
        final Path path = new Path(parquetFilePath);

        FilterPredicate filterPredicate = this.buildFilter(condition.getComplexCondition());
        FilterCompat.Filter filter = null;
        if (filterPredicate != null) {
            ParquetInputFormat.setFilterPredicate(conf,filterPredicate);
            filter = ParquetInputFormat.getFilter(conf);
        }

        final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);

        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

        HDFSParquetFileReader reader = null;
        try {

            reader = new HDFSParquetFileReader(conf, path, fileNamePattern, null, projectedFields, includePathColumn, false, filter);

            Object parquetData = reader.read();
            while (parquetData != null && !isStopRequested()) {
                result.addRow( (Object[])parquetData, projectedFields);

                parquetData = reader.read();
            }

         

        } catch (final Exception e) {
            LOG.error("Error accessing Parquet file", e);
            throw new CustomWrapperException("Error accessing Parquet file: " + e.getMessage(), e);

        } finally {
            try {
                if (reader != null ) {
                    reader.close();
                }
            } catch (final IOException e) {
                LOG.error("Error releasing the reader", e);
            }

        }
    }

    public FilterPredicate buildFilter (final CustomWrapperCondition vdpCondition) throws CustomWrapperException {

        if (vdpCondition != null) {
            if (vdpCondition.isAndCondition()) {
                CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) vdpCondition;
                List<FilterPredicate> filterPredicates  = new ArrayList<FilterPredicate>();
                for (CustomWrapperCondition condition : andCondition.getConditions()) {
                    if (condition.isSimpleCondition()) {
                        FilterPredicate filterPredicate = this.generateSimpleFilterPredicate(condition);
                        filterPredicates.add(filterPredicate);
                    } else {
                        FilterPredicate filterPredicateComplex = this.buildFilter(condition);
                        filterPredicates.add(filterPredicateComplex);
                    }
                }
                if (filterPredicates.size() >= 2) {
                    FilterPredicate filterPredicate = filterPredicates.get(0);
                    for (int i = 1; i < filterPredicates.size(); i++) {
                        filterPredicate = and(filterPredicate,filterPredicates.get(i));
                    }
                    return  filterPredicate;
                } else {
                    throw new CustomWrapperException("Error obtaining the FilterPredicate for the and condition \"" + andCondition.toString() + "\"");
                }
            } else if (vdpCondition.isOrCondition()) {
                CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) vdpCondition;
                List<FilterPredicate> filterPredicates  = new ArrayList<FilterPredicate>();
                for (CustomWrapperCondition condition : orCondition.getConditions()) {
                    if (condition.isSimpleCondition()) {
                        FilterPredicate filterPredicate = this.generateSimpleFilterPredicate(condition);
                        filterPredicates.add(filterPredicate);
                    } else {
                        FilterPredicate filterPredicateComplex = this.buildFilter(condition);
                        filterPredicates.add(filterPredicateComplex);
                    }
                }
                if (filterPredicates.size() >= 2) {
                    FilterPredicate filterPredicate = filterPredicates.get(0);
                    for (int i = 1; i < filterPredicates.size(); i++) {
                        filterPredicate = or(filterPredicate,filterPredicates.get(i));
                    }
                    return  filterPredicate;
                } else {
                    throw new CustomWrapperException("Error obtaining the FilterPredicate for the and condition \"" + orCondition.toString() + "\"");
                }
            } else if (vdpCondition.isSimpleCondition()) {
                return this.generateSimpleFilterPredicate(vdpCondition);
            } else {
                throw new CustomWrapperException("Condition \"" + vdpCondition.toString() + "\" not allowed");
            }

        } else {
            return null;
        }
        /*ParquetInputFormat.setFilterPredicate(conf, lt(intColumn("id"),5));
        FilterCompat.Filter filter = ParquetInputFormat.getFilter(conf);*/
    }

    private FilterPredicate generateSimpleFilterPredicate(CustomWrapperCondition condition) {

        CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) condition;
        String operator = simpleCondition.getOperator();
        FilterPredicate filterPredicate = null;

        String field = simpleCondition.getField().toString();
        for (CustomWrapperExpression expression : simpleCondition.getRightExpression()) {
            if (expression.isSimpleExpression()) {
                CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression)expression;
                switch (operator) {
                    case CustomWrapperCondition.OPERATOR_EQ:
                        if (simpleExpression.getValue() instanceof Integer) {
                            filterPredicate = eq(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Long) {
                            filterPredicate = eq(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Double) {
                            filterPredicate = eq(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Float) {
                            filterPredicate = eq(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof String) {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = eq(binaryColumn(field),Binary.fromString(s));
                        } else if (simpleExpression.getValue() instanceof Boolean) {
                            filterPredicate = eq(booleanColumn(field),Boolean.valueOf(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Date) {
                            //We add 1 day because the 1970-01-01 counter starts at 0
                            filterPredicate = eq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof java.sql.Date) {
                            filterPredicate = eq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof Short) {
                            filterPredicate = eq(intColumn(field),((Short)simpleExpression.getValue()).intValue());
                        } else {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = eq(binaryColumn(field),Binary.fromString(s));
                        }
                        break;
                    case CustomWrapperCondition.OPERATOR_NE:
                        if (simpleExpression.getValue() instanceof Integer) {
                            filterPredicate = notEq(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Long) {
                            filterPredicate = notEq(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Double) {
                            filterPredicate = notEq(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Float) {
                            filterPredicate = notEq(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof String) {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = notEq(binaryColumn(field),Binary.fromString(s));
                        } else if (simpleExpression.getValue() instanceof Boolean) {
                            filterPredicate = notEq(booleanColumn(field),Boolean.valueOf(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Date) {
                            filterPredicate = notEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof java.sql.Date) {
                            filterPredicate = notEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof Short) {
                            filterPredicate = eq(intColumn(field),((Short)simpleExpression.getValue()).intValue());
                        } else {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = notEq(binaryColumn(field),Binary.fromString(s));
                        }
                        break;
                    case CustomWrapperCondition.OPERATOR_LT:
                        if (simpleExpression.getValue() instanceof Integer) {
                            filterPredicate = lt(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Long) {
                            filterPredicate = lt(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Double) {
                            filterPredicate = lt(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Float) {
                            filterPredicate = lt(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof String) {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = lt(binaryColumn(field),Binary.fromString(s));
                        } else if (simpleExpression.getValue() instanceof Date) {
                            filterPredicate = lt(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof java.sql.Date) {
                            filterPredicate = lt(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof Short) {
                            filterPredicate = eq(intColumn(field),((Short)simpleExpression.getValue()).intValue());
                        } else {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = lt(binaryColumn(field),Binary.fromString(s));
                        }
                        break;
                    case CustomWrapperCondition.OPERATOR_LE:
                        if (simpleExpression.getValue() instanceof Integer) {
                            filterPredicate = ltEq(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Long) {
                            filterPredicate = ltEq(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Double) {
                            filterPredicate = ltEq(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Float) {
                            filterPredicate = ltEq(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof String) {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = ltEq(binaryColumn(field),Binary.fromString(s));
                        } else if (simpleExpression.getValue() instanceof Date) {
                            filterPredicate = ltEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof java.sql.Date) {
                            filterPredicate = ltEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof Short) {
                            filterPredicate = ltEq(intColumn(field),((Short)simpleExpression.getValue()).intValue());
                        } else {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = ltEq(binaryColumn(field),Binary.fromString(s));
                        }
                        break;
                    case CustomWrapperCondition.OPERATOR_GT:
                        if (simpleExpression.getValue() instanceof Integer) {
                            filterPredicate = gt(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Long) {
                            filterPredicate = gt(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Double) {
                            filterPredicate = gt(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Float) {
                            filterPredicate = gt(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof String) {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = gt(binaryColumn(field),Binary.fromString(s));
                        } else if (simpleExpression.getValue() instanceof Date) {
                            filterPredicate = gt(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof java.sql.Date) {
                            filterPredicate = gt(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof Short) {
                            filterPredicate = gt(intColumn(field),((Short)simpleExpression.getValue()).intValue());
                        } else {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = gt(binaryColumn(field),Binary.fromString(s));
                        }
                        break;
                    case CustomWrapperCondition.OPERATOR_GE:
                        if (simpleExpression.getValue() instanceof Integer) {
                            filterPredicate = gtEq(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Long) {
                            filterPredicate = gtEq(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Double) {
                            filterPredicate = gtEq(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof Float) {
                            filterPredicate = gtEq(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                        } else if (simpleExpression.getValue() instanceof String) {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = gtEq(binaryColumn(field),Binary.fromString(s));
                        } else if (simpleExpression.getValue() instanceof Date) {
                            filterPredicate = gtEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof java.sql.Date) {
                            filterPredicate = gtEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                        } else if (simpleExpression.getValue() instanceof Short) {
                            filterPredicate = gtEq(intColumn(field),((Short)simpleExpression.getValue()).intValue());
                        } else {
                            String s = simpleExpression.getValue().toString();
                            filterPredicate = gtEq(binaryColumn(field),Binary.fromString(s));
                        }
                        break;
                    default:
                }
            }
        }
        return filterPredicate;
    }

}
