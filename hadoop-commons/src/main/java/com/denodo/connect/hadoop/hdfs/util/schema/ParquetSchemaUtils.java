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
package com.denodo.connect.hadoop.hdfs.util.schema;


import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_EQ;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_NE;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.type.ParquetTypeUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;

public class ParquetSchemaUtils {

    /**
     * This is the list of names that can have the repeated element list
     */
    private static final List<String> LIST_VALUES = new ArrayList<>(Arrays.asList("list"));
    
    /**
     * This is the list of names that can have the repeated element map 
     */
    private static final List<String> MAP_VALUES = new ArrayList<>(Arrays.asList("map", "key_value"));

    /**
     * This method build the parquet schema
     */
    public static  SchemaElement buildSchema(final GroupType group, final SchemaElement schemaElement) {

        for (final Type field : group.getFields()) {
            try {
                final boolean isNullable = field.getRepetition() != Repetition.REQUIRED;
                if (field.isPrimitive()) {
                    addPrimitiveType(schemaElement, field, isNullable);
                } else if (ParquetTypeUtils.isGroup(field)) {
                    addGroupType(schemaElement, field, isNullable);
                } else if (ParquetTypeUtils.isList(field)) {
                    addListType(schemaElement, field, isNullable);
                } else if (ParquetTypeUtils.isMap(field)) {
                    addMapType(schemaElement, field, isNullable);
                }
            } catch (final ClassCastException e) {
                throw new IllegalArgumentException("Unsupported type: " + field);
            }
        }
        return schemaElement;
    }

    /**
     * Method to add the map types to the schema
     */
    private static void addMapType(final SchemaElement schemaElement, final Type field, final boolean isNullable) {
        try {
            // This element have as OriginalType MAP. The standard MAPS in parquet should have repeated as next element
            final Type nextFieldMap = field.asGroupType().getFields() != null && !field.asGroupType().getFields()
                .isEmpty() ? field.asGroupType().getFields().get(0) : null;
            
            if (nextFieldMap != null
                    && MAP_VALUES.contains(nextFieldMap.getName()) 
                    && nextFieldMap.getRepetition() != null
                    && nextFieldMap.getRepetition() == Repetition.REPEATED
                    && nextFieldMap.asGroupType().getFields() != null
                    && nextFieldMap.asGroupType().getFieldCount() == 2
                    && nextFieldMap.asGroupType().getFields().get(0).getRepetition() == Repetition.REQUIRED) {

                //Create the map schema 
                final SchemaElement schemaElementMap = new SchemaElement(field.getName(), Map.class, isNullable);
                
                //Take the key field 
                final Type nextFieldKey = nextFieldMap.asGroupType().getFields().get(0);
                //Take the value field and if is nullable information
                final Type nextFieldValue = nextFieldMap.asGroupType().getFields().get(1);
                final boolean nextFieldIsNullable = nextFieldValue.getRepetition() != Repetition.REQUIRED;
                
                //Add the key to the schema
                if (nextFieldKey.isPrimitive()) {
                    addPrimitiveType(schemaElementMap, nextFieldKey, nextFieldIsNullable);
                } else {
                    throw new IllegalArgumentException("ERROR when building the schema. The list element " + field.getName()
                        + " doesn't have a valid format. Key should be a primitive type");
                }
                //Add the value to the schema
                if (nextFieldValue.isPrimitive()) {
                    addPrimitiveType(schemaElementMap, nextFieldValue, nextFieldIsNullable);
                } else {
                    schemaElementMap.add(buildSchema(nextFieldValue.asGroupType(), schemaElementMap));
                }
                schemaElement.add(schemaElementMap);
            } else {
                throw new IllegalArgumentException("ERROR when building the schema. The list element " + field.getName() + " doesn't have a valid format");
            }
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("ERROR when building the schema for type: " + field);
        }
    }

    /**
     * Method to add the list types to the schema
     */
    private static void addListType(final SchemaElement schemaElement, final Type field, final boolean isNullable) {
        try {
            //This element have as OriginalType LIST. The standard LISTS in parquet should have repeated as next element
            final Type nextFieldList = field.asGroupType().getFields() != null && !field.asGroupType().getFields()
                .isEmpty() ? field.asGroupType().getFields().get(0) : null;
            
            if (nextFieldList != null && LIST_VALUES.contains(nextFieldList.getName()) && nextFieldList.getRepetition() != null
                    && nextFieldList.getRepetition() == Repetition.REPEATED && nextFieldList.asGroupType().getFields() != null) {
                
                final SchemaElement schemaElementList = new SchemaElement(field.getName(), List.class, isNullable);
                schemaElement.add(buildSchema(nextFieldList.asGroupType(), schemaElementList));
            } else {
                //For Backward-compatibility is necessary to add code here. Two list elements is not necessary. 
                throw new IllegalArgumentException("ERROR when building the schema. The list element " + field.getName() + " doesn't have a valid format");
            }
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("ERROR when building the schema for type: " + field);
        }
    }

    /**
     * Method to add the group types to the schema
     */
    private static void addGroupType(final SchemaElement schemaElement, final Type field, final boolean isNullable) {
        try {
            final SchemaElement schemaElementGroup = new SchemaElement(field.getName(), Object.class, isNullable);
            schemaElement.add(buildSchema(field.asGroupType(), schemaElementGroup));
        }  catch (final ClassCastException e) {
            throw new IllegalArgumentException("ERROR when building the schema for type: " + field);
        }
    }

    /**
     * Method to add the primitive types to the schema
     */
    private static void addPrimitiveType(final SchemaElement schemaElement, final Type field, final boolean isNullable) {
        schemaElement.add(new SchemaElement(field.getName(), ParquetTypeUtils.toJava(field), field.asPrimitiveType().getPrimitiveTypeName(), isNullable));
    }

    public static FilterPredicate buildFilter(final CustomWrapperCondition vdpCondition, final SchemaElement schema) throws CustomWrapperException {

        if (vdpCondition != null) {
            if (vdpCondition.isAndCondition()) {
                final CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) vdpCondition;
                final List<FilterPredicate> filterPredicates  = new ArrayList<>();
                for (final CustomWrapperCondition condition : andCondition.getConditions()) {
                    if (condition.isSimpleCondition()) {
                        final FilterPredicate filterPredicate = generateSimpleFilterPredicate(condition, schema);
                        filterPredicates.add(filterPredicate);
                    } else {
                        final FilterPredicate filterPredicateComplex = buildFilter(condition, schema);
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
                    throw new CustomWrapperException("Error obtaining the FilterPredicate for the and condition \"" + andCondition.toString() + '"');
                }
            } else if (vdpCondition.isOrCondition()) {
                final CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) vdpCondition;
                final List<FilterPredicate> filterPredicates  = new ArrayList<>();
                for (final CustomWrapperCondition condition : orCondition.getConditions()) {
                    if (condition.isSimpleCondition()) {
                        final FilterPredicate filterPredicate = generateSimpleFilterPredicate(condition, schema);
                        filterPredicates.add(filterPredicate);
                    } else {
                        final FilterPredicate filterPredicateComplex = buildFilter(condition, schema);
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
                    throw new CustomWrapperException("Error obtaining the FilterPredicate for the and condition \"" + orCondition.toString() + '"');
                }
            } else if (vdpCondition.isSimpleCondition()) {
                return generateSimpleFilterPredicate(vdpCondition, schema);
            } else {
                throw new CustomWrapperException("Condition \"" + vdpCondition.toString() + "\" not allowed");
            }

        } else {
            return null;
        }
    }

    private static SchemaElement getSchemaField(final String field, final SchemaElement schema) {
        if (schema != null) {
            final String[] fields = field.split("\\.",2);
            for (final SchemaElement element : schema.getElements()){
                if (fields.length == 1 && element.getName().equals(fields[0])) {
                    return element;
                } else if (fields.length > 1 && element.getName().equals(fields[0])) {
                    return getSchemaField(fields[1],element);
                }
            }
        }
        return null;
    }

    private static FilterPredicate generateSimpleFilterPredicate(final CustomWrapperCondition condition, final SchemaElement schema) {
        final CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) condition;
        final String operator = simpleCondition.getOperator();
        FilterPredicate filterPredicate = null;
        final String field = simpleCondition.getField().toString();
        final SchemaElement element = getSchemaField(field, schema);
        for (final CustomWrapperExpression expression : simpleCondition.getRightExpression()) {
            if (expression.isSimpleExpression()) {
                final CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression)expression;
                final boolean simpleExpressionValueIsNull = simpleExpression.getValue() == null;
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
                        filterPredicate = simpleExpressionValueIsNull ? eq(binaryColumn(field),null) : eq(binaryColumn(field), Binary.fromString(simpleExpression.getValue().toString()));
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

    public static List<BlockMetaData> getRowGroups(final Configuration configuration, final Path filePath) throws IOException {
        List<BlockMetaData> rowGroups  = null;
        try (final ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, configuration))) {

            rowGroups = parquetFileReader.getRowGroups();
        }
        return rowGroups;
    }

}
