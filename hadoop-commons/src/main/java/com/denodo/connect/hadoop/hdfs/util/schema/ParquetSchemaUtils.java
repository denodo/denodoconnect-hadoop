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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.filter2.predicate.Operators.SupportsEqNotEq;
import org.apache.parquet.filter2.predicate.Operators.SupportsLtGt;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.thrift.Option.Some;

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
    private static final List<String> LIST_VALUES = Collections.singletonList("list");
    
    /**
     * This is the list of names that can have the repeated element map 
     */
    private static final List<String> MAP_VALUES = Arrays.asList("map", "key_value");

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

    public static FilterPredicate buildFilter(final CustomWrapperCondition vdpCondition, final MessageType parquetSchema)
        throws CustomWrapperException {

        if (vdpCondition != null) {
            if (vdpCondition.isAndCondition()) {
                final CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) vdpCondition;
                final List<FilterPredicate> filterPredicates  = new ArrayList<>();
                for (final CustomWrapperCondition condition : andCondition.getConditions()) {
                    if (condition.isSimpleCondition()) {
                        final FilterPredicate filterPredicate = buildSimpleFilter(condition, parquetSchema);
                        filterPredicates.add(filterPredicate);
                    } else {
                        final FilterPredicate filterPredicateComplex = buildFilter(condition, parquetSchema);
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
                        final FilterPredicate filterPredicate = buildSimpleFilter(condition, parquetSchema);
                        filterPredicates.add(filterPredicate);
                    } else {
                        final FilterPredicate filterPredicateComplex = buildFilter(condition, parquetSchema);
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
                return buildSimpleFilter(vdpCondition, parquetSchema);
            } else {
                throw new CustomWrapperException("Condition \"" + vdpCondition.toString() + "\" not allowed");
            }

        } else {
            return null;
        }
    }

    private static FilterPredicate buildSimpleFilter(final CustomWrapperCondition condition, final MessageType parquetSchema) {

        FilterPredicate filterPredicate = null;

        final CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) condition;
        final String operator = simpleCondition.getOperator();
        final String fieldName = simpleCondition.getField().toString();
        final Type fieldType = getFieldType(fieldName, parquetSchema);

        for (final CustomWrapperExpression expression : simpleCondition.getRightExpression()) {

            if (expression.isSimpleExpression() && fieldType.isPrimitive()) {
                final CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression) expression;
                final Pair<Column, Comparable> filterValues = getFilterValues(simpleExpression, fieldName, fieldType);

                switch (operator) {
                    case OPERATOR_EQ:
                        filterPredicate = eq(upcastEqNotEq(filterValues.getLeft()), filterValues.getRight());
                        break;
                    case OPERATOR_NE:
                        filterPredicate = notEq(upcastEqNotEq(filterValues.getLeft()), filterValues.getRight());
                        break;
                    case OPERATOR_LT:
                        filterPredicate = lt(upcastLtGt(filterValues.getLeft()), filterValues.getRight());
                        break;
                    case OPERATOR_LE:
                        filterPredicate = ltEq(upcastLtGt(filterValues.getLeft()), filterValues.getRight());
                        break;
                    case OPERATOR_GT:
                        filterPredicate = gt(upcastLtGt(filterValues.getLeft()), filterValues.getRight());
                        break;
                    case OPERATOR_GE:
                        filterPredicate = gtEq(upcastLtGt(filterValues.getLeft()), filterValues.getRight());
                        break;
                }
            }
        }

        return filterPredicate;
    }

    // Helper methods because intersection types in casts are not supported at java 7.
    private static <Q extends Column & SupportsEqNotEq> Q upcastEqNotEq(final Object in) {
        return (Q) in;
    }

    private static <Q extends Column & SupportsLtGt> Q upcastLtGt(final Object in) {
        return (Q) in;
    }
    //

    private static Type getFieldType(final String fieldName, final MessageType parquetSchema) {
        return parquetSchema.getType(fieldName);
    }

    private static Pair<Column, Comparable> getFilterValues(final CustomWrapperSimpleExpression simpleExpression,
        final String fieldName, final Type fieldType) {

        Pair<Column, Comparable> pair = null;
        final PrimitiveTypeName primitiveTypeName = fieldType.asPrimitiveType().getPrimitiveTypeName();
        if (simpleExpression.getValue() instanceof Date) {
            // Adds 1 day because the 1970-01-01 counter starts at 0
            pair = Pair.of((Column) intColumn(fieldName), (Comparable) (((Date) simpleExpression.getValue()).getTime() / (24 * 60 * 60 * 1000) + 1));

        } else if (simpleExpression.getValue() instanceof BigDecimal) {
            final int scale = fieldType.asPrimitiveType().getDecimalMetadata().getScale();
            pair = Pair.of((Column) intColumn(fieldName), (Comparable) ((BigDecimal) simpleExpression.getValue()).setScale(scale).unscaledValue());

        } else if (simpleExpression.getValue() instanceof Integer || simpleExpression.getValue() instanceof Short
                || PrimitiveTypeName.INT32.equals(primitiveTypeName)) {
                pair = Pair.of((Column) intColumn(fieldName), (Comparable) simpleExpression.getValue());

        } else if (simpleExpression.getValue() instanceof Long || PrimitiveTypeName.INT64.equals(primitiveTypeName)) {
            pair = Pair.of((Column) longColumn(fieldName), (Comparable) simpleExpression.getValue());

        } else if (simpleExpression.getValue() instanceof Double || PrimitiveTypeName.DOUBLE.equals(primitiveTypeName)) {
            pair = Pair.of((Column) doubleColumn(fieldName), (Comparable) simpleExpression.getValue());

        } else if (simpleExpression.getValue() instanceof Float || PrimitiveTypeName.FLOAT.equals(primitiveTypeName)) {
            pair = Pair.of((Column) floatColumn(fieldName), (Comparable) simpleExpression.getValue());

        } else if (simpleExpression.getValue() instanceof Boolean || PrimitiveTypeName.BOOLEAN.equals(primitiveTypeName)) {
            pair = Pair.of((Column) booleanColumn(fieldName), (Comparable) simpleExpression.getValue());

        } else {
            pair = Pair.of((Column) binaryColumn(fieldName),
                simpleExpression.getValue() == null ? null : (Comparable) Binary.fromString(simpleExpression.getValue().toString()));

        }


        return pair;
    }

    public static List<BlockMetaData> getRowGroups(final Configuration configuration, final Path filePath) throws IOException {
        List<BlockMetaData> rowGroups  = null;
        try (final ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, configuration))) {

            rowGroups = parquetFileReader.getRowGroups();
        }
        return rowGroups;
    }

}
