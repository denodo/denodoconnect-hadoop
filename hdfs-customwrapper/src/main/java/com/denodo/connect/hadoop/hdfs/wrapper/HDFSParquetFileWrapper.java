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


        final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);

        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

        HDFSParquetFileReader reader = null;
        try {

            reader = new HDFSParquetFileReader(conf, path, fileNamePattern, null, projectedFields, includePathColumn, false, null);

            SchemaElement schema = null;
            if (hasNullValueInCondition(condition.getComplexCondition())) {
                schema = reader.getSchema(conf);
            }

            FilterPredicate filterPredicate = this.buildFilter(condition.getComplexCondition(), schema);
            FilterCompat.Filter filter = null;
            if (filterPredicate != null) {
                ParquetInputFormat.setFilterPredicate(conf,filterPredicate);
                filter = ParquetInputFormat.getFilter(conf);
            }

            reader.setFilter(filter);

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



    public FilterPredicate buildFilter (final CustomWrapperCondition vdpCondition, SchemaElement schema) throws CustomWrapperException {

        if (vdpCondition != null) {
            if (vdpCondition.isAndCondition()) {
                CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) vdpCondition;
                List<FilterPredicate> filterPredicates  = new ArrayList<FilterPredicate>();
                for (CustomWrapperCondition condition : andCondition.getConditions()) {
                    if (condition.isSimpleCondition()) {
                        FilterPredicate filterPredicate = this.generateSimpleFilterPredicate(condition, schema);
                        filterPredicates.add(filterPredicate);
                    } else {
                        FilterPredicate filterPredicateComplex = this.buildFilter(condition, schema);
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
                        FilterPredicate filterPredicate = this.generateSimpleFilterPredicate(condition, schema);
                        filterPredicates.add(filterPredicate);
                    } else {
                        FilterPredicate filterPredicateComplex = this.buildFilter(condition, schema);
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
                return this.generateSimpleFilterPredicate(vdpCondition, schema);
            } else {
                throw new CustomWrapperException("Condition \"" + vdpCondition.toString() + "\" not allowed");
            }

        } else {
            return null;
        }
    }

    public boolean hasNullValueInCondition (final CustomWrapperCondition vdpCondition) throws CustomWrapperException {

        if (vdpCondition != null) {
            if (vdpCondition.isAndCondition()) {
                CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) vdpCondition;
                for (CustomWrapperCondition condition : andCondition.getConditions()) {
                    if (condition.isSimpleCondition()) {
                        if (hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) vdpCondition)) return true;
                    } else {
                        if (this.hasNullValueInCondition(condition)) return true;
                    }
                }
            } else if (vdpCondition.isOrCondition()) {
                CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) vdpCondition;
                for (CustomWrapperCondition condition : orCondition.getConditions()) {
                    if (condition.isSimpleCondition()) {
                        if (hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) vdpCondition)) return true;
                    } else {
                        if (this.hasNullValueInCondition(condition)) return true;
                    }
                }
            } else if (vdpCondition.isSimpleCondition()) {
                if (hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) vdpCondition)) return true;
            } else {
                throw new CustomWrapperException("Condition \"" + vdpCondition.toString() + "\" not allowed");
            }

        } else {
            return false;
        }
        return  false;
    }

    private boolean hasNullValueInSimpleCondition(CustomWrapperSimpleCondition vdpCondition) {
        CustomWrapperSimpleCondition simpleCondition = vdpCondition;
        for (CustomWrapperExpression expression : simpleCondition.getRightExpression()) {
            if (expression.isSimpleExpression()) {
                CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression) expression;
                if (simpleExpression.getValue() == null) {
                    return true;
                }
            }
        }
        return false;
    }

    private SchemaElement getSchemaField(String field, SchemaElement schema) {
        if (schema != null) {
            String[] fields = field.split("\\.",2);
            for (SchemaElement element : schema.getElements()){
                if (fields.length == 1 && element.getName().equals(fields[0])) {
                    return element;
                } else if (fields.length > 1 && element.getName().equals(fields[0])) {
                    return getSchemaField(fields[1],element);
                }
            }
        }
        return null;
    }

    private FilterPredicate generateSimpleFilterPredicate(CustomWrapperCondition condition, SchemaElement schema) {
        CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) condition;
        String operator = simpleCondition.getOperator();
        FilterPredicate filterPredicate = null;
        String field = simpleCondition.getField().toString();
        SchemaElement element = getSchemaField(field, schema);
        for (CustomWrapperExpression expression : simpleCondition.getRightExpression()) {
            if (expression.isSimpleExpression()) {
                CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression)expression;
                boolean simpleExpressionValueIsNull = simpleExpression.getValue() == null;
                if (simpleExpression.getValue() instanceof Integer || (simpleExpressionValueIsNull && element != null && element.getType().equals(Integer.class))) {
                    if (operator.equals(OPERATOR_EQ)) {
                        filterPredicate = simpleExpressionValueIsNull ? eq(intColumn(field),null) : eq(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_NE)) {
                        filterPredicate = simpleExpressionValueIsNull ? notEq(intColumn(field),null) : notEq(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LT)) {
                        filterPredicate = simpleExpressionValueIsNull ? lt(intColumn(field),null) : lt(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LE)) {
                        filterPredicate = simpleExpressionValueIsNull ? ltEq(intColumn(field),null) : ltEq(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GT)) {
                        filterPredicate = simpleExpressionValueIsNull ? gt(intColumn(field),null) : gt(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GE)) {
                        filterPredicate = simpleExpressionValueIsNull ? gtEq(intColumn(field),null) : gtEq(intColumn(field),Integer.parseInt(simpleExpression.getValue().toString()));
                    }
                } else if (simpleExpression.getValue() instanceof Long || (simpleExpressionValueIsNull && element != null && element.getType().equals(Long.class))) {
                    if (operator.equals(OPERATOR_EQ)) {
                        filterPredicate = simpleExpressionValueIsNull ? eq(longColumn(field),null) : eq(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_NE)) {
                        filterPredicate = simpleExpressionValueIsNull ? notEq(longColumn(field),null) : notEq(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LT)) {
                        filterPredicate = simpleExpressionValueIsNull ? lt(longColumn(field),null) : lt(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LE)) {
                        filterPredicate = simpleExpressionValueIsNull ? ltEq(longColumn(field),null) : ltEq(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GT)) {
                        filterPredicate = simpleExpressionValueIsNull ? gt(longColumn(field),null) : gt(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GE)) {
                        filterPredicate = simpleExpressionValueIsNull ? gtEq(longColumn(field),null) : gtEq(longColumn(field),Long.parseLong(simpleExpression.getValue().toString()));
                    }
                } else if (simpleExpression.getValue() instanceof Double || (simpleExpressionValueIsNull && element != null && element.getType().equals(Double.class))) {
                    if (operator.equals(OPERATOR_EQ)) {
                        filterPredicate = simpleExpressionValueIsNull ? eq(doubleColumn(field),null) : eq(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_NE)) {
                        filterPredicate = simpleExpressionValueIsNull ? notEq(doubleColumn(field),null) : notEq(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LT)) {
                        filterPredicate = simpleExpressionValueIsNull ? lt(doubleColumn(field),null) : lt(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LE)) {
                        filterPredicate = simpleExpressionValueIsNull ? ltEq(doubleColumn(field),null) : ltEq(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GT)) {
                        filterPredicate = simpleExpressionValueIsNull ? gt(doubleColumn(field),null) : gt(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GE)) {
                        filterPredicate = simpleExpressionValueIsNull ? gtEq(doubleColumn(field),null) : gtEq(doubleColumn(field),Double.parseDouble(simpleExpression.getValue().toString()));
                    }
                } else if (simpleExpression.getValue() instanceof Float || (simpleExpressionValueIsNull && element != null && element.getType().equals(Float.class))) {
                    if (operator.equals(OPERATOR_EQ)) {
                        filterPredicate = simpleExpressionValueIsNull ? eq(floatColumn(field),null) : eq(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_NE)) {
                        filterPredicate = simpleExpressionValueIsNull ? notEq(floatColumn(field),null) : notEq(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LT)) {
                        filterPredicate = simpleExpressionValueIsNull ? lt(floatColumn(field),null) : lt(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LE)) {
                        filterPredicate = simpleExpressionValueIsNull ? ltEq(floatColumn(field),null) : ltEq(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GT)) {
                        filterPredicate = simpleExpressionValueIsNull ? gt(floatColumn(field),null) : gt(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GE)) {
                        filterPredicate = simpleExpressionValueIsNull ? gtEq(floatColumn(field),null) : gtEq(floatColumn(field),Float.parseFloat(simpleExpression.getValue().toString()));
                    }
                } else if (simpleExpression.getValue() instanceof String || (simpleExpressionValueIsNull && element != null && element.getType().equals(String.class))) {
                    if (operator.equals(OPERATOR_EQ)) {
                        filterPredicate = simpleExpressionValueIsNull ? eq(binaryColumn(field),null) : eq(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_NE)) {
                        filterPredicate = simpleExpressionValueIsNull ? notEq(binaryColumn(field),null) : notEq(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LT)) {
                        filterPredicate = simpleExpressionValueIsNull ? lt(binaryColumn(field),null) : lt(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LE)) {
                        filterPredicate = simpleExpressionValueIsNull ? ltEq(binaryColumn(field),null) : ltEq(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GT)) {
                        filterPredicate = simpleExpressionValueIsNull ? gt(binaryColumn(field),null) : gt(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GE)) {
                        filterPredicate = simpleExpressionValueIsNull ? gtEq(binaryColumn(field),null) : gtEq(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    }
                } else if (simpleExpression.getValue() instanceof Boolean || (simpleExpressionValueIsNull && element != null && element.getType().equals(Boolean.class))) {
                    if (operator.equals(OPERATOR_EQ)) {
                        filterPredicate = simpleExpressionValueIsNull ? eq(booleanColumn(field),null) : eq(booleanColumn(field),Boolean.valueOf(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_NE)) {
                        filterPredicate = simpleExpressionValueIsNull ? notEq(booleanColumn(field),null) : notEq(booleanColumn(field),Boolean.valueOf(simpleExpression.getValue().toString()));
                    }
                } else if (simpleExpression.getValue() instanceof Date || (simpleExpressionValueIsNull && element != null && element.getType().equals(Date.class))) {
                    if (operator.equals(OPERATOR_EQ)) {
                        //We add 1 day because the 1970-01-01 counter starts at 0
                        filterPredicate = simpleExpressionValueIsNull ? eq(intColumn(field),null) : eq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    } else if (operator.equals(OPERATOR_NE)) {
                        filterPredicate = simpleExpressionValueIsNull ? notEq(intColumn(field),null) : notEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    } else if (operator.equals(OPERATOR_LT)) {
                        filterPredicate = simpleExpressionValueIsNull ? lt(intColumn(field),null) : lt(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    } else if (operator.equals(OPERATOR_LE)) {
                        filterPredicate = simpleExpressionValueIsNull ? ltEq(intColumn(field),null) : ltEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    } else if (operator.equals(OPERATOR_GT)) {
                        filterPredicate = simpleExpressionValueIsNull ? gt(intColumn(field),null) : gt(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    } else if (operator.equals(OPERATOR_GE)) {
                        filterPredicate = simpleExpressionValueIsNull ? gtEq(intColumn(field),null) : gtEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    }
                } else if (simpleExpression.getValue() instanceof java.sql.Date || (simpleExpressionValueIsNull && element != null && element.getType().equals(java.sql.Date.class))) {
                    if (operator.equals(OPERATOR_EQ)) {
                        filterPredicate = simpleExpressionValueIsNull ? eq(intColumn(field),null) : eq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    } else if (operator.equals(OPERATOR_NE)) {
                        filterPredicate = simpleExpressionValueIsNull ? notEq(intColumn(field),null) : notEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    } else if (operator.equals(OPERATOR_LT)) {
                        filterPredicate = simpleExpressionValueIsNull ? lt(intColumn(field),null) : lt(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    } else if (operator.equals(OPERATOR_LE)) {
                        filterPredicate = simpleExpressionValueIsNull ? ltEq(intColumn(field),null) : ltEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    } else if (operator.equals(OPERATOR_GT)) {
                        filterPredicate = simpleExpressionValueIsNull ? gt(intColumn(field),null) : gt(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    } else if (operator.equals(OPERATOR_GE)) {
                        filterPredicate = simpleExpressionValueIsNull ? gtEq(intColumn(field),null) : gtEq(intColumn(field),Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (1000 * 60 * 60 * 24) + 1));
                    }
                } else if (simpleExpression.getValue() instanceof Short || (simpleExpressionValueIsNull && element != null && element.getType().equals(Short.class))) {
                    if (operator.equals(OPERATOR_EQ)) {
                        filterPredicate = simpleExpressionValueIsNull ? eq(intColumn(field),null) : eq(intColumn(field), ((Short) simpleExpression.getValue()).intValue());
                    } else if (operator.equals(OPERATOR_NE)) {
                        filterPredicate = simpleExpressionValueIsNull ? notEq(intColumn(field),null) : notEq(intColumn(field), ((Short) simpleExpression.getValue()).intValue());
                    } else if (operator.equals(OPERATOR_LT)) {
                        filterPredicate = simpleExpressionValueIsNull ? lt(intColumn(field),null) : lt(intColumn(field), ((Short) simpleExpression.getValue()).intValue());
                    } else if (operator.equals(OPERATOR_LE)) {
                        filterPredicate = simpleExpressionValueIsNull ? ltEq(intColumn(field),null) : ltEq(intColumn(field), ((Short) simpleExpression.getValue()).intValue());
                    } else if (operator.equals(OPERATOR_GT)) {
                        filterPredicate = simpleExpressionValueIsNull ? gt(intColumn(field),null) : gt(intColumn(field), ((Short) simpleExpression.getValue()).intValue());
                    } else if (operator.equals(OPERATOR_GE)) {
                        filterPredicate = simpleExpressionValueIsNull ? gtEq(intColumn(field),null) : gtEq(intColumn(field), ((Short) simpleExpression.getValue()).intValue());
                    }
                } else {
                    if (operator.equals(OPERATOR_EQ)) {
                        filterPredicate = simpleExpressionValueIsNull ? eq(binaryColumn(field),null) : eq(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_NE)) {
                        filterPredicate = simpleExpressionValueIsNull ? notEq(binaryColumn(field),null) : notEq(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LT)) {
                        filterPredicate = simpleExpressionValueIsNull ? lt(binaryColumn(field),null) : lt(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_LE)) {
                        filterPredicate = simpleExpressionValueIsNull ? ltEq(binaryColumn(field),null) : ltEq(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GT)) {
                        filterPredicate = simpleExpressionValueIsNull ? gt(binaryColumn(field),null) : gt(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    } else if (operator.equals(OPERATOR_GE)) {
                        filterPredicate = simpleExpressionValueIsNull ? gtEq(binaryColumn(field),null) : gtEq(binaryColumn(field),Binary.fromString(simpleExpression.getValue().toString()));
                    }
                }
            }
        }
        return filterPredicate;
    }

}
