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
package com.denodo.connect.hadoop.hdfs.util.io;

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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.filter2.predicate.Operators.SupportsEqNotEq;
import org.apache.parquet.filter2.predicate.Operators.SupportsLtGt;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.denodo.connect.hadoop.hdfs.util.type.DataComparator;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;

public final class ConditionUtils {

    private ConditionUtils() {

    }

    public static FilterPredicate buildFilter(final CustomWrapperCondition vdpCondition, final MessageType parquetSchema) {

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
                    throw new IllegalArgumentException("Error obtaining the FilterPredicate for the and condition \"" + andCondition + '"');
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
                    throw new IllegalArgumentException("Error obtaining the FilterPredicate for the and condition \"" + orCondition + '"');
                }
            } else if (vdpCondition.isSimpleCondition()) {
                return buildSimpleFilter(vdpCondition, parquetSchema);
            } else {
                throw new IllegalArgumentException("Condition \"" + vdpCondition + "\" not allowed");
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
                        filterPredicate = eq((Column<Comparable> & SupportsEqNotEq) filterValues.getLeft(), filterValues.getRight());
                        break;
                    case OPERATOR_NE:
                        filterPredicate = notEq((Column<Comparable> & SupportsEqNotEq) filterValues.getLeft(), filterValues.getRight());
                        break;
                    case OPERATOR_LT:
                        filterPredicate = lt((Column<Comparable> & SupportsLtGt) filterValues.getLeft(), filterValues.getRight());
                        break;
                    case OPERATOR_LE:
                        filterPredicate = ltEq((Column<Comparable> & SupportsLtGt) filterValues.getLeft(), filterValues.getRight());
                        break;
                    case OPERATOR_GT:
                        filterPredicate = gt((Column<Comparable> & SupportsLtGt) filterValues.getLeft(), filterValues.getRight());
                        break;
                    case OPERATOR_GE:
                        filterPredicate = gtEq((Column<Comparable> & SupportsLtGt) filterValues.getLeft(), filterValues.getRight());
                        break;
                }
            }
        }

        return filterPredicate;
    }

    private static Type getFieldType(final String fieldName, final MessageType parquetSchema) {
        return parquetSchema.getType(fieldName);
    }

    private static Pair<Column, Comparable> getFilterValues(final CustomWrapperSimpleExpression simpleExpression,
        final String fieldName, final Type fieldType) {

        Pair<Column, Comparable> pair = null;
        final PrimitiveTypeName primitiveTypeName = fieldType.asPrimitiveType().getPrimitiveTypeName();
        if (simpleExpression.getValue() instanceof Date) {
            // Adds 1 day because the 1970-01-01 counter starts at 0
            pair = Pair.of(intColumn(fieldName), Math.toIntExact(((Date) simpleExpression.getValue()).getTime() / (24 * 60 * 60 * 1000) + 1));

        } else if (simpleExpression.getValue() instanceof BigDecimal) {
            final int scale = fieldType.asPrimitiveType().getDecimalMetadata().getScale();
            pair = Pair.of(intColumn(fieldName), ((BigDecimal) simpleExpression.getValue()).setScale(scale).unscaledValue().intValueExact());

        } else if (simpleExpression.getValue() instanceof Integer || simpleExpression.getValue() instanceof Short
            || PrimitiveTypeName.INT32.equals(primitiveTypeName)) {
            pair = Pair.of(intColumn(fieldName), (Integer) simpleExpression.getValue());

        } else if (simpleExpression.getValue() instanceof Long || PrimitiveTypeName.INT64.equals(primitiveTypeName)) {
            pair = Pair.of(longColumn(fieldName), (Long) simpleExpression.getValue());

        } else if (simpleExpression.getValue() instanceof Double || PrimitiveTypeName.DOUBLE.equals(primitiveTypeName)) {
            pair = Pair.of(doubleColumn(fieldName), (Double) simpleExpression.getValue());

        } else if (simpleExpression.getValue() instanceof Float || PrimitiveTypeName.FLOAT.equals(primitiveTypeName)) {
            pair = Pair.of(floatColumn(fieldName), (Float) simpleExpression.getValue());

        } else if (simpleExpression.getValue() instanceof Boolean || PrimitiveTypeName.BOOLEAN.equals(primitiveTypeName)) {
            pair = Pair.of(booleanColumn(fieldName), (Boolean) simpleExpression.getValue());

        } else {
            pair = Pair.of(binaryColumn(fieldName),
                simpleExpression.getValue() == null ? null : Binary.fromString(simpleExpression.getValue().toString()));
        }


        return pair;
    }

    /**
     * Get the simple conditions, excluding the ones with compound fields (compound fields are not included in projections).
     *
     */
    public static Collection<String> getSimpleConditionFields(final CustomWrapperCondition condition) {

        final Collection<String> simpleConditions = new LinkedHashSet<>();
        if (condition != null) {
            if (condition.isAndCondition()) {
                final CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) condition;
                for (final CustomWrapperCondition c : andCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        final String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        if (fieldName.split("\\.").length == 1) {
                            simpleConditions.add(fieldName);
                        }
                    } else {
                        simpleConditions.addAll(getSimpleConditionFields(c));
                    }
                }
            } else if (condition.isOrCondition()) {
                final CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) condition;
                for (final CustomWrapperCondition c : orCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        final String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        if (fieldName.split("\\.").length == 1) {
                            simpleConditions.add(fieldName);
                        }
                    } else {
                        simpleConditions.addAll(getSimpleConditionFields(c));
                    }
                }
            } else if (condition.isSimpleCondition()) {
                final String fieldName = ((CustomWrapperSimpleCondition) condition).getField().toString();
                if (fieldName.split("\\.").length == 1) {
                    simpleConditions.add(fieldName);
                }

            } else {
                throw new IllegalArgumentException("Condition \"" + condition + "\" not allowed");
            }
        }

        return simpleConditions;
    }

    public static CustomWrapperConditionHolder removeConditions(final CustomWrapperConditionHolder conditionHolder,
        final List<String> conditionsToRemove) {

        final CustomWrapperCondition newCondition = removeConditions(conditionHolder.getComplexCondition(),
            conditionsToRemove);

        return new CustomWrapperConditionHolder(newCondition);
    }

    private static CustomWrapperCondition removeConditions(final CustomWrapperCondition condition,
        final Collection<String> conditionsToRemove) {

        CustomWrapperCondition newCondition = null;

        if (condition != null) {
            if (condition.isAndCondition()) {
                final List<CustomWrapperCondition> newConditions = new ArrayList<>();
                final CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) condition;
                for (final CustomWrapperCondition c : andCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        final String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        if (!conditionsToRemove.contains(fieldName)) {
                            newConditions.add(c);
                        }
                    } else {
                        final CustomWrapperCondition result = removeConditions(c, conditionsToRemove);
                        if (result != null) {
                            newConditions.add(result);
                        }
                    }
                }

                if (!newConditions.isEmpty()) {
                    newCondition = newConditions.size() == 1 ? newConditions.get(0) : new CustomWrapperAndCondition(newConditions);
                }


            } else if (condition.isOrCondition()) {
                final CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) condition;
                final LinkedList<CustomWrapperCondition> newConditions = new LinkedList<>();
                for (final CustomWrapperCondition c : orCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        final String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        if (!conditionsToRemove.contains(fieldName)) {
                            newConditions.add(c);
                        }

                    } else {
                        final CustomWrapperCondition result = removeConditions(c, conditionsToRemove);
                        if (result != null) {
                            newConditions.add(result);
                        }
                    }
                }

                if (!newConditions.isEmpty()) {
                    newCondition = newConditions.size() == 1 ? newConditions.get(0) : new CustomWrapperOrCondition(newConditions);
                }

            } else if (condition.isSimpleCondition()) {
                final String fieldName = ((CustomWrapperSimpleCondition) condition).getField().toString();
                if (!conditionsToRemove.contains(fieldName)) {
                    newCondition = condition;
                }

            } else {
                throw new IllegalArgumentException("Condition \"" + condition + "\" not allowed");
            }
        }

        return newCondition;
    }


    public static boolean evalPartitionCondition(final CustomWrapperCondition condition,
        final Map<String, Comparable> partitionFieldValues) {

        boolean match = true;

        if (condition != null) {
            if (condition.isAndCondition()) {
                final CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) condition;
                for (final CustomWrapperCondition c : andCondition.getConditions()) {
                    if (match) {
                        if (c.isSimpleCondition()) {
                            final String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                            if (partitionFieldValues.containsKey(fieldName)) {
                                match = evalSimplePartitionCondition(((CustomWrapperSimpleCondition) c),
                                    partitionFieldValues.get(fieldName));
                            }
                        } else {
                            match = evalPartitionCondition(c, partitionFieldValues);
                        }
                    }
                }
            } else if (condition.isOrCondition()) {
                match = false;
                final CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) condition;
                for (final CustomWrapperCondition c : orCondition.getConditions()) {
                    if (!match) {
                        if (c.isSimpleCondition()) {
                            final String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                            if (partitionFieldValues.containsKey(fieldName)) {
                                match = evalSimplePartitionCondition(((CustomWrapperSimpleCondition) c),
                                    partitionFieldValues.get(fieldName));
                            } else {
                                match = true; // it is a condition that does not involve partition fields
                            }
                        } else {
                            match = evalPartitionCondition(c, partitionFieldValues);
                        }
                    }
                }
            } else if (condition.isSimpleCondition()) {
                final String fieldName = ((CustomWrapperSimpleCondition) condition).getField().toString();
                if (partitionFieldValues.containsKey(fieldName)) {
                    match = evalSimplePartitionCondition(((CustomWrapperSimpleCondition) condition), partitionFieldValues.get(fieldName));
                }

            } else {
                throw new IllegalArgumentException("Condition \"" + condition + "\" not allowed");
            }
        }

        return match;
    }

    private static boolean evalSimplePartitionCondition(final CustomWrapperSimpleCondition simpleCondition,
        final Comparable partitionValue) {

        boolean match = true;

        final String operator = simpleCondition.getOperator();
        final CustomWrapperExpression[] expressions = simpleCondition.getRightExpression();

        for (final CustomWrapperExpression expression : expressions) {

            if (expression.isSimpleExpression()) {
                final CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression) expression;

                switch (operator) {
                    case OPERATOR_EQ:
                        match = DataComparator.compare(partitionValue, simpleExpression.getValue()) == 0;
                        break;
                    case OPERATOR_NE:
                        match = DataComparator.compare(partitionValue, simpleExpression.getValue()) != 0;
                        break;
                    case OPERATOR_LT:
                        match = DataComparator.compare(partitionValue, simpleExpression.getValue()) < 0;
                        break;
                    case OPERATOR_LE:
                        match = DataComparator.compare(partitionValue, simpleExpression.getValue()) <= 0;
                        break;
                    case OPERATOR_GT:
                        match = DataComparator.compare(partitionValue, simpleExpression.getValue()) > 0;
                        break;
                    case OPERATOR_GE:
                        match = DataComparator.compare(partitionValue, simpleExpression.getValue()) >= 0;
                        break;
                }
            }
        }

        return match;
    }

}