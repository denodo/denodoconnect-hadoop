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
package com.denodo.connect.hadoop.hdfs.reader;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.condition.*;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaUtils;
import com.denodo.connect.hadoop.hdfs.util.type.ParquetTypeUtils;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.*;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GE;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;


public class HDFSParquetFileReader extends AbstractHDFSFileReader {

    private static final  Logger LOG = LoggerFactory.getLogger(HDFSParquetFileReader.class);

    private ParquetReader<Group> dataFileReader;
    private List<CustomWrapperFieldExpression> projectedFields;
    private CustomWrapperConditionHolder condition;
    private MessageType parquetSchema;
    private FilterCompat.Filter filter;
    private boolean hasNullValueInConditions;
    private List<String> conditionFields;

    public FilterCompat.Filter getFilter() {
        return filter;
    }

    public void setFilter(FilterCompat.Filter filter) {
        this.filter = filter;
    }

    public boolean getHasNullValueInConditions() {
        return hasNullValueInConditions;
    }

    public List<String> getConditionFields() {
        return conditionFields;
    }

    public HDFSParquetFileReader(final Configuration conf, final Path path, final String finalNamePattern, final String user,
    		final List<CustomWrapperFieldExpression> projectedFields, final CustomWrapperConditionHolder condition,
            final boolean includePathColumn, boolean getSchemaParameters, FilterCompat.Filter filter)
            throws IOException, InterruptedException {

        super(conf, path, finalNamePattern, user, includePathColumn);
        this.projectedFields = projectedFields;
        this.condition = condition;
        this.filter = filter;
        this.hasNullValueInConditions = false;
        this.conditionFields = condition != null ? this.getFieldsNameAndCheckNullConditions(condition.getComplexCondition()) : null;
    }

    @Override
    public void doOpenReader(final FileSystem fileSystem, final Path path,
            final Configuration configuration) throws IOException {

            if (this.parquetSchema == null) {
                this.parquetSchema = getProjectedParquetSchema(configuration, path);
            } else {
                this.schemaWithProjectedAndConditionFields(this.parquetSchema);
            }
        
        // Sets the expected schema so Parquet could validate that all files (e.g. in a directory) follow this schema. 
        // If the schema is not set Parquet will use the schema contained in each Parquet file 
        configuration.set(ReadSupport.PARQUET_READ_SCHEMA, this.parquetSchema.toString());
        
        final GroupReadSupport groupReadSupport = new GroupReadSupport();
        if (filter != null) {
            this.dataFileReader = ParquetReader.builder(groupReadSupport, path).withConf(configuration).withFilter(this.filter).build();
        } else {
            this.dataFileReader = ParquetReader.builder(groupReadSupport, path).withConf(configuration).build();
        }
    }

    public SchemaElement getSchema(final Configuration configuration) throws IOException {

        final Path filePath = nextFilePath();
        this.parquetSchema = getParquetSchema(configuration, filePath);
        
        final boolean isMandatory = this.parquetSchema.getRepetition() == Repetition.REQUIRED  ? true : false;
        final SchemaElement schemaElement = new SchemaElement(this.parquetSchema.getName(), Object.class, isMandatory); 

        return ParquetSchemaUtils.buildSchema(this.parquetSchema, schemaElement);

    }

    private MessageType getParquetSchema(final Configuration configuration, final Path filePath) throws IOException {

        final ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, filePath, ParquetMetadataConverter.NO_FILTER);

        final MessageType schema = readFooter.getFileMetaData().getSchema();

        return schema;
    }

    private MessageType getProjectedParquetSchema(final Configuration configuration, final Path filePath) throws IOException {
        
        final ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, filePath, ParquetMetadataConverter.NO_FILTER);
        
        final MessageType schema = readFooter.getFileMetaData().getSchema();

        this.schemaWithProjectedAndConditionFields(schema);

        return schema;
    }

    private void schemaWithProjectedAndConditionFields(MessageType schema) {
        if (this.projectedFields != null || this.conditionFields != null) {
            List<Integer> schemaFieldsToDelete  = new ArrayList<Integer>();

            List<Type> schemaWithProjectionAndConditionFields = new ArrayList<Type>();
            for (CustomWrapperFieldExpression projectedField : this.projectedFields) {
                for (Type schemaField : schema.getFields()) {
                    if (schemaField.getName().equals(projectedField.getName())) {
                        schemaWithProjectionAndConditionFields.add(schemaField);
                    }
                }
            }
            for (String conditionField : this.conditionFields) {
                for (Type schemaField : schema.getFields()) {
                    if (schemaField.getName().equals(conditionField)) {
                        schemaWithProjectionAndConditionFields.add(schemaField);
                    }
                }
            }
            if (schema.getFields().size() != schemaWithProjectionAndConditionFields.size()) {
                schema.getFields().removeAll(schema.getFields());
                schema.getFields().addAll(schemaWithProjectionAndConditionFields);
            }
        }
    }

    /**
     * Get the condition field names excluding the projected field names and compound fields (compound fields is not included in projections).
     * This method also initialize the hasNullValueInConditions variable
     *
     * @param condition
     * @return list with fields
     */
    private List<String> getFieldsNameAndCheckNullConditions(CustomWrapperCondition condition) throws IOException {
        List<String> conditionFields = new ArrayList<String>();
        if (condition != null) {
            if (condition.isAndCondition()) {
                CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) condition;
                for (CustomWrapperCondition c : andCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        //We only add fieldName to conditionFields if it is not a compound type and if it is not already included.
                        if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                            conditionFields.add(fieldName);
                        }
                        if (this.hasNullValueInConditions == false && hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) c)) {
                            this.hasNullValueInConditions = true;
                        }
                    } else {
                        List<String> fieldsName = this.getFieldsNameAndCheckNullConditions(c);
                        for (String fieldName : fieldsName) {
                            if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                                conditionFields.add(fieldName);
                            }
                        }
                    }
                }
            } else if (condition.isOrCondition()) {
                CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) condition;
                for (CustomWrapperCondition c : orCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                            conditionFields.add(fieldName);
                        }
                        if (this.hasNullValueInConditions == false && hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) c)) {
                            this.hasNullValueInConditions = true;
                        }
                    } else {
                        List<String> fieldsName = this.getFieldsNameAndCheckNullConditions(c);
                        for (String fieldName : fieldsName) {
                            if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                                conditionFields.add(fieldName);
                            }
                        }
                    }
                }
            } else if (condition.isSimpleCondition()) {
                String fieldName = ((CustomWrapperSimpleCondition) condition).getField().toString();
                if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                    conditionFields.add(fieldName);
                }
                if (this.hasNullValueInConditions == false && hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) condition)) {
                    this.hasNullValueInConditions = true;
                }
            } else {
                throw new IOException("Condition \"" + condition.toString() + "\" not allowed");
            }
        }
        for (CustomWrapperFieldExpression projectedField : this.projectedFields) {
            if(conditionFields.contains(projectedField.getName())) {
                conditionFields.remove(projectedField.getName());
            }
        }
        return  conditionFields;
    }

    /**
     * Get if a simple condition evaluate a null value
     *
     * @param vdpCondition
     * @return
     */
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

    @Override
    public Object doRead() throws IOException {
        
        final Group data = this.dataFileReader.read();
        if (data != null) {
            return readParquetLogicalTypes(data, this.projectedFields);
        }

        return null;

    }

    @Override
    public void closeReader() throws IOException {
        if (this.dataFileReader != null) {
            this.dataFileReader.close();
        }
    }

    /**
     * Method for read all the parquet types
     * @return Record with the fields
     */
    private static Object[] readParquetLogicalTypes(final Group datum, final List<CustomWrapperFieldExpression> projectedFields)
            throws IOException {
        final List<Type> fields = datum.getType().getFields();
        final Object[] vdpRecord = new Object[fields.size()];
        int i = 0;
        for (final Type field : fields) {
            vdpRecord[i] = readTypes(datum, projectedFields, field);
            i++;
        }
        return vdpRecord;
    }

    /**
     * Method for read parquet types. The method does the validation to know what type needs to be read.
     */
    private static Object readTypes(final Group datum, final List<CustomWrapperFieldExpression> projectedFields, final Type valueList)
            throws IOException {
        
        if (valueList.isPrimitive()) {
            return readPrimitive(datum, valueList);
        }
        
        if (ParquetTypeUtils.isGroup(valueList)) {
            return readGroup(datum, projectedFields, valueList);
        } else if (ParquetTypeUtils.isMap(valueList)) {
            return readMap(datum, projectedFields, valueList);
        } else if (ParquetTypeUtils.isList(valueList)) {
            return readList(datum, projectedFields, valueList);
        } else {
            LOG.error("Type of the field " + valueList.toString() + ", does not supported by the custom wrapper ");
            throw new IOException("Type of the field " + valueList.toString() + ", does not supported by the custom wrapper ");
        }
    }

    /**
     * Read method for parquet primitive types
     */
    private static Object readPrimitive(final Group datum, final Type field) throws IOException {
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            final PrimitiveTypeName   primitiveTypeName = field.asPrimitiveType().getPrimitiveTypeName();
            try{
                if(primitiveTypeName.equals(PrimitiveTypeName.BINARY)) {
                    if(field.getOriginalType()!=null){
                        if(field.getOriginalType().equals(OriginalType.UTF8)){
                            return datum.getString(field.getName(), 0);
                        } else if(field.getOriginalType().equals(OriginalType.JSON)){
                            return datum.getString(field.getName(), 0);
                        } else if(field.getOriginalType().equals(OriginalType.BSON)){
                            return datum.getString(field.getName(), 0);
                        } else{
                            return datum.getBinary(field.getName(), 0).getBytes();
                        }
                    }
                    return datum.getBinary(field.getName(), 0).getBytes();
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.BOOLEAN)) {
                    return  datum.getBoolean(field.getName(), 0);
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.DOUBLE)) {
                    return datum.getDouble(field.getName(), 0);
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.FLOAT)) {
                    return datum.getFloat(field.getName(), 0);
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.INT32)) {
                    if (OriginalType.DECIMAL.equals(field.getOriginalType())) {
                        final int scale = field.asPrimitiveType().getDecimalMetadata().getScale();
                        return new BigDecimal(BigInteger.valueOf(datum.getInteger(field.getName(), 0)), scale);
                    } else if (OriginalType.DATE.equals(field.getOriginalType())) {
                        //   DATE fields really holds the number of days since 1970-01-01
                        final int days = datum.getInteger(field.getName(),0);
                        // We need to add one day because the index start in 0.
                        final long daysMillis = TimeUnit.DAYS.toMillis(days+1);
                        return new Date(daysMillis);
                    } else {
                        return datum.getInteger(field.getName(), 0);
                    }
                    
                } else if(primitiveTypeName.equals(PrimitiveTypeName.INT64)) {
                    //we dont differentiate INT64 from TIMESTAMP_MILLIS original types
                    return datum.getLong(field.getName(), 0);
                    
                } else if(primitiveTypeName.equals(PrimitiveTypeName.INT96)) {
                    Binary binary = datum.getInt96(field.getName(), 0);
                    long timestampMillis = ParquetTypeUtils.int96ToTimestampMillis(binary);
                    return new Date(timestampMillis);
                    
                } else if(primitiveTypeName.equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
                    if (OriginalType.DECIMAL.equals(field.getOriginalType())) {
                        final int scale = field.asPrimitiveType().getDecimalMetadata().getScale();
                        return new BigDecimal(new BigInteger(datum.getBinary(field.getName(), 0).getBytes()), scale);
                    }
                    return datum.getBinary(field.getName(), 0).getBytes();
                } else{
                    LOG.error("Type of the field "+ field.toString()+", does not supported by the custom wrapper ");
                    throw new IOException("Type of the field "+ field.toString()+", does not supported by the custom wrapper ");
                }
            } catch(final RuntimeException e){
                LOG.warn("It was a error reading data", e);
            }
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        return null;
    }
    
    /**
     * Read method for parquet group types
     * @return Record with the fields
     */
    private static Object readGroup(final Group datum, final List<CustomWrapperFieldExpression> projectedFields, final Type field) throws IOException {
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            Object[] vdpGroupRecord = new Object[field.asGroupType().getFields().size()];
            vdpGroupRecord = readParquetLogicalTypes(datum.getGroup(field.getName(), 0), projectedFields);
            return vdpGroupRecord;
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        LOG.trace("The group " + field.getName() + " doesn't exist in the data");
        return null;
    }
    
    /**
     * Read method for parquet list types
     * @return Array with the fields
     */
    private static Object readList(final Group datum, final List<CustomWrapperFieldExpression> projectedFields, final Type field) throws IOException {
        //This only works with the last parquet version 1.5.0 Previous versions maybe don't have a valid format.
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            try {
                final Group datumGroup = datum.getGroup(field.getName(), 0);
                if (datumGroup.getFieldRepetitionCount(0) > 0) {
                    final Object[][] vdpArrayRecord = new Object[datumGroup.getFieldRepetitionCount(0)][1];
                    for (int j = 0; j < datumGroup.getFieldRepetitionCount(0); j++) {
                        final Group datumList = datumGroup.getGroup(0, j)!= null ? datumGroup.getGroup(0, j) : null;
                        if (datumList != null && datumList.getType().getFields().size() > 0) {
                            vdpArrayRecord[j][0] = readTypes(datumList, projectedFields, datumList.getType().getFields().get(0));
                        } else {
                            throw new IOException("The list element " + field.getName() + " don't have a valid format");
                        }
                    }
                    return vdpArrayRecord;
                }
            } catch (final ClassCastException e) {
                throw new IOException("The list element " + field.getName() + " don't have a valid format",e);
            }
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        LOG.trace("The list " + field.getName() + " doesn't exist in the data");
        return null;
    }
    
    /**
     * Read method for parquet map types
     * @return Array with the fields
     */
    private static Object readMap(final Group datum, final List<CustomWrapperFieldExpression> projectedFields, final Type field) throws IOException {
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            try {
                final Group datumGroup = datum.getGroup(field.getName(), 0);
                if (datumGroup.getFieldRepetitionCount(0) > 0) {
                    final Object[][] vdpArrayRecord = new Object[datumGroup.getFieldRepetitionCount(0)][2];
                    for (int j = 0; j < datumGroup.getFieldRepetitionCount(0); j++) {
                        final Group datumMap = datumGroup.getGroup(0, j) != null ? datumGroup.getGroup(0, j) : null;
                        if (datumMap != null && datumMap.getType().getFields().size() > 1) {
                            vdpArrayRecord[j][0] = readPrimitive(datumMap,datumMap.getType().getFields().get(0));
                            vdpArrayRecord[j][1] = readTypes(datumMap, projectedFields, datumMap.getType().getFields().get(0));
                        } else {
                            throw new IOException("The list element " + field.getName() + " don't have a valid format");
                        }
                    }
                    return vdpArrayRecord;
                }
            } catch (final ClassCastException e) {
                throw new IOException("The map element " + field.getName() + " don't have a valid format",e);
            }
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        LOG.trace("The map " + field.getName() + " doesn't exist in the data");
        return null;
    }
    
}
