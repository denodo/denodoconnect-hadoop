/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2012, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hbase.commons.naming.ParameterNaming;
import com.denodo.connect.hadoop.hbase.util.HbaseUtil;
import com.denodo.vdb.catalog.operator.Operator;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperNotCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class HBaseConnector extends AbstractCustomWrapper {

    private static final Logger logger = Logger.getLogger(HBaseConnector.class);

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return new CustomWrapperInputParameter[] {
                new CustomWrapperInputParameter(ParameterNaming.CONF_HBASE_IP, true),
                new CustomWrapperInputParameter(ParameterNaming.CONF_HBASE_PORT, false),
                new CustomWrapperInputParameter(ParameterNaming.CONF_TABLE_NAME, true),
                new CustomWrapperInputParameter(ParameterNaming.CONF_TABLE_MAPPING, true),

        };
    }

    private boolean stopRequested = false;

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        final CustomWrapperConfiguration configuration = new CustomWrapperConfiguration();
        configuration.setDelegateProjections(false);
        configuration.setDelegateNotConditions(true);
        configuration.setDelegateOrConditions(true);
        configuration.setDelegateRightLiterals(true);

        configuration.setAllowedOperators(new String[] { CustomWrapperCondition.OPERATOR_EQ,
                CustomWrapperCondition.OPERATOR_NE, CustomWrapperCondition.OPERATOR_REGEXPLIKE,
                CustomWrapperCondition.OPERATOR_ISNULL, CustomWrapperCondition.OPERATOR_ISNOTNULL,
                CustomWrapperCondition.OPERATOR_IN, CustomWrapperCondition.OPERATOR_CONTAINS,
                CustomWrapperCondition.OPERATOR_CONTAINSAND, CustomWrapperCondition.OPERATOR_CONTAINSOR,
                CustomWrapperCondition.OPERATOR_LIKE, CustomWrapperCondition.OPERATOR_ISCONTAINED,
        });

        return configuration;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(
            final Map<String, String> inputValues) throws CustomWrapperException {

        final String mapping = inputValues.get(ParameterNaming.CONF_TABLE_MAPPING);
        try {
            logger.info("Start getSchemaParameters hbase");
            final Map<String, List<HBaseColumnDetails>> mappingMap = HbaseUtil.parseMapping(mapping);

            final ArrayList<CustomWrapperSchemaParameter> rows = new ArrayList<CustomWrapperSchemaParameter>();

            // row ID
            rows.add(new CustomWrapperSchemaParameter(ParameterNaming.COL_ROWKEY, java.sql.Types.VARCHAR, null,
                    true, CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false));

            // output schema based on the provided json
            for (final String col : mappingMap.keySet()) {
                final ArrayList<CustomWrapperSchemaParameter> subrows = new ArrayList<CustomWrapperSchemaParameter>();
                for (final HBaseColumnDetails subrowData : mappingMap.get(col)) {
                    CustomWrapperSchemaParameter subrow = null;

                    subrow = new CustomWrapperSchemaParameter(subrowData.getName(), HbaseUtil.getSQLType(subrowData
                            .getType()),
                            null,
                            true, CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false);
                    subrows.add(subrow);

                }
                final CustomWrapperSchemaParameter row = new CustomWrapperSchemaParameter(col, java.sql.Types.STRUCT,
                        subrows.toArray(new CustomWrapperSchemaParameter[] {}));
                rows.add(row);
            }

            return rows.toArray(new CustomWrapperSchemaParameter[] {});

        } catch (final Exception e) {
            e.printStackTrace();
            throw new CustomWrapperException("Error in mapping format: " + e.getMessage());
        }

    }

    @Override
    public void run(final CustomWrapperConditionHolder condition,
            final List<CustomWrapperFieldExpression> projectedFields,
            final CustomWrapperResult result, final Map<String, String> inputValues)
            throws CustomWrapperException {
        logger.info("Start run hbase");
        Map<String, List<HBaseColumnDetails>> mappingMap;
        try {
            final String mapping = inputValues.get(ParameterNaming.CONF_TABLE_MAPPING);
            mappingMap = HbaseUtil.parseMapping(mapping);
        } catch (final Exception e) {
            throw new CustomWrapperException("Error in mapping format: " + e);
        }

        // Connects to HBase server
        final String tableName = inputValues.get(ParameterNaming.CONF_TABLE_NAME);
        final Configuration config = HBaseConfiguration.create();
        config.clear();
        config.set(ParameterNaming.CONF_ZOOKEEPER_QUORUM, inputValues.get(ParameterNaming.CONF_HBASE_IP));
        final String port = inputValues.get(ParameterNaming.CONF_HBASE_PORT);
        if (port != null) {
            config.set(ParameterNaming.CONF_ZOOKEEPER_CLIENTPORT, port);
        }
        HTable table;
        try {
            // Get table metadata
            table = new HTable(config, tableName);
            final Set<byte[]> families = table.getTableDescriptor().getFamiliesKeys();

            final CustomWrapperCondition conditionComplex = condition.getComplexCondition();
            final Scan scan = new Scan();
            if ((conditionComplex != null)) {
                /*
                 * COMPLEX FILTER DELEGATION, CACHING, AND OTHER COMPLEX HBASE SCANNING FEATURES COULD BE ADDED HERE
                 */
                for (final String family : mappingMap.keySet()) {
                    for (final HBaseColumnDetails subrowData : mappingMap.get(family)) {
                        scan.addColumn(family.getBytes(), subrowData.getName().getBytes());
                    }
                }
                final Filter filter = buildFilterFromCustomWrapperConditionn(conditionComplex, false);
                scan.setFilter(filter);

            } else {
                // For performance reasons, just add to the scanner the families and qualifiers
                // specified in the mapping
                for (final String family : mappingMap.keySet()) {
                    for (final HBaseColumnDetails subrowData : mappingMap.get(family)) {
                        scan.addColumn(family.getBytes(), subrowData.getName().getBytes());
                    }
                }

            }

            final ResultScanner scanner = table.getScanner(scan);
            try {
                for (final Result resultRow : scanner) {
                    // Stop the scan if requested from outside
                    if (this.stopRequested) {
                        break;
                    }
                    // add the row to the output
                    final Object[] rowArray = processRow(resultRow, mappingMap, families);
                    result.addRow(rowArray, HbaseUtil.getGenericOutputpStructure(mappingMap));
                }

            } finally {
                scanner.close();
            }
        } catch (final Exception e) {
            throw new CustomWrapperException("Error accessing the HBase table: " + e);
        }

    }

    private static Filter buildFilterFromCustomWrapperConditionn(final CustomWrapperCondition conditionComplex,
            final boolean not) {
        if (conditionComplex.isAndCondition()) {
            FilterList.Operator operator;
            if (not) {
                operator = FilterList.Operator.MUST_PASS_ONE;
            } else {
                operator = FilterList.Operator.MUST_PASS_ALL;
            }
            final FilterList filterList = new FilterList(operator);
            final CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) conditionComplex;
            for (final CustomWrapperCondition condition : andCondition.getConditions()) {
                filterList.addFilter(buildFilterFromCustomWrapperConditionn(condition, not));

            }
            return filterList;
        } else if (conditionComplex.isOrCondition()) {
            FilterList.Operator operator;
            if (!not) {
                operator = FilterList.Operator.MUST_PASS_ONE;
            } else {
                operator = FilterList.Operator.MUST_PASS_ALL;
            }
            final FilterList filterList = new FilterList(operator);
            final CustomWrapperOrCondition conditionOr = (CustomWrapperOrCondition) conditionComplex;
            for (final CustomWrapperCondition condition : conditionOr.getConditions()) {
                filterList.addFilter(buildFilterFromCustomWrapperConditionn(condition, not));

            }
            return filterList;
        } else if (conditionComplex.isNotCondition()) {
            final CustomWrapperNotCondition conditionNot = (CustomWrapperNotCondition) conditionComplex;

            return buildFilterFromCustomWrapperConditionn(conditionNot.getCondition(), true);
        } else {
            final CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) conditionComplex;

            final CustomWrapperFieldExpression conditionExpression = (CustomWrapperFieldExpression) simpleCondition
                    .getField();
            final String familyColumn = conditionExpression.getName();
            final CustomWrapperExpression[] rightExpresion = simpleCondition.getRightExpression();

            String value = "";
            if ((rightExpresion != null) && (rightExpresion.length > 0)) {
                value = rightExpresion[0].toString();
            }

            Filter filter = null;

            String column = "";
            // Creating filter according to operator
            if (conditionExpression.hasSubFields()) {
                final List<CustomWrapperFieldExpression> list = conditionExpression.getSubFields();
                column = list.get(0).toString();

            }
            if (simpleCondition.getOperator().equals(Operator.EQUALS_TO)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                        Bytes.toBytes(column),
                        operator, new BinaryComparator(Bytes.toBytes(value)));
                if (!not) {
                    // If you want that rows, that has a column with value null,be filtered, it is necessary to enable
                    // FilterIFMissing
                    filterColumn.setFilterIfMissing(true);
                }
                filter = filterColumn;

            } else if (simpleCondition.getOperator().equals(Operator.NOT_EQUALS_TO)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.NOT_EQUAL;
                } else {
                    operator = CompareOp.EQUAL;
                }
                final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                        Bytes.toBytes(column),
                        operator, new BinaryComparator(Bytes.toBytes(value)));
                if (not) {
                    filterColumn.setFilterIfMissing(true);
                }
                filter = filterColumn;
            } else if (simpleCondition.getOperator().equals(Operator.REGEXP_LIKE)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                        Bytes.toBytes(column),
                        operator, new RegexStringComparator(value));
                if (!not) {
                    filterColumn.setFilterIfMissing(true);
                }
                filter = filterColumn;
            } else if (simpleCondition.getOperator().equals(Operator.LIKE)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                        Bytes.toBytes(column),
                        operator, new RegexStringComparator(HbaseUtil.getRegExpformLike(value)));
                if (!not) {
                    filterColumn.setFilterIfMissing(true);
                }
                filter = filterColumn;

            } else if (simpleCondition.getOperator().equals(Operator.IS_NULL)) {
                if (!not) {
                    filter = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            CompareOp.NO_OP, new NullComparator());
                } else {
                    final QualifierFilter columnFilter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(
                            Bytes.toBytes(column)));
                    filter = columnFilter;
                }

            } else if (simpleCondition.getOperator().equals(Operator.IS_NOT_NULL)) {
                if (!not) {
                    final QualifierFilter columnFilter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(
                            Bytes.toBytes(column)));
                    filter = columnFilter;
                } else {
                    filter = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            CompareOp.NO_OP, new NullComparator());
                }
            } else if (simpleCondition.getOperator().equals(Operator.IN)) {
                FilterList.Operator operator;
                CompareOp compareOp;
                if (!not) {
                    operator = FilterList.Operator.MUST_PASS_ONE;
                    compareOp = CompareOp.EQUAL;
                } else {
                    operator = FilterList.Operator.MUST_PASS_ALL;
                    compareOp = CompareOp.NOT_EQUAL;
                }
                final FilterList filterList = new FilterList(operator);
                if (rightExpresion != null) {
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                                Bytes.toBytes(familyColumn),
                                Bytes.toBytes(column),
                                compareOp, new BinaryComparator(Bytes.toBytes(factor.toString())));
                        filterColumn.setFilterIfMissing(true);
                        if (!not) {
                            filterColumn.setFilterIfMissing(true);
                        }
                        filterList.addFilter(filterColumn);
                    }
                }
                filter = filterList;
            } else if (simpleCondition.getOperator().equals(Operator.CONTAINS)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                        Bytes.toBytes(column),
                        operator, new RegexStringComparator(value));
                if (!not) {
                    filterColumn.setFilterIfMissing(true);
                }
                filter = filterColumn;
            } else if (simpleCondition.getOperator().equals(Operator.IS_CONTAINED)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                        Bytes.toBytes(column),
                        operator, new RegexStringComparator(value));
                if (!not) {
                    filterColumn.setFilterIfMissing(true);
                }
                filter = filterColumn;
            } else if (simpleCondition.getOperator().equals(Operator.CONTAINS_AND)) {
                FilterList.Operator operator;
                CompareOp compareOp;
                if (!not) {
                    operator = FilterList.Operator.MUST_PASS_ALL;
                    compareOp = CompareOp.EQUAL;
                } else {
                    operator = FilterList.Operator.MUST_PASS_ONE;
                    compareOp = CompareOp.NOT_EQUAL;
                }
                final FilterList filterList = new FilterList(operator);
                if (rightExpresion != null) {
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                                Bytes.toBytes(familyColumn),
                                Bytes.toBytes(column),
                                compareOp, new RegexStringComparator(factor.toString()));
                        if (!not) {
                            filterColumn.setFilterIfMissing(true);
                        }
                        filterList.addFilter(filterColumn);
                    }
                }
                filter = filterList;
            } else if (simpleCondition.getOperator().equals(Operator.CONTAINS_OR)) {
                FilterList.Operator operator;
                CompareOp compareOp;
                if (!not) {
                    operator = FilterList.Operator.MUST_PASS_ONE;
                    compareOp = CompareOp.EQUAL;
                } else {
                    operator = FilterList.Operator.MUST_PASS_ALL;
                    compareOp = CompareOp.NOT_EQUAL;
                }
                final FilterList filterList = new FilterList(operator);
                if (rightExpresion != null) {
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                                Bytes.toBytes(familyColumn),
                                Bytes.toBytes(column),
                                compareOp, new RegexStringComparator(factor.toString()));
                        if (!not) {
                            filterColumn.setFilterIfMissing(true);
                        }
                        filterList.addFilter(filterColumn);
                    }
                }
                filter = filterList;
            } else {
                filter = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                        Bytes.toBytes(column),
                        CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
            }

            return filter;
        }
    }

    private static Object[] processRow(final Result resultSet, final Map<String, List<HBaseColumnDetails>> mappingMap,
            final Set<byte[]> families) {
        if (logger.isDebugEnabled()) {
            logger.debug("HBASE row: " + resultSet.toString());
        }

        // Iterates through the families if they are mapped
        final Object[] rowArray = new Object[mappingMap.keySet().size() + 1];

        int i = 0;
        for (final String mappingFaimilyName : mappingMap.keySet()) {

            // the row contains the mapped family
            if (families.contains(mappingFaimilyName.getBytes())) {
                final NavigableMap<byte[], byte[]> familyMap = resultSet.getFamilyMap(mappingFaimilyName.getBytes());

                final Set<byte[]> keys = familyMap.keySet();
                final Object[] subrowArray = new Object[mappingMap.get(mappingFaimilyName).size()];
                int j = 0;
                // And fills the sub-rows
                for (final HBaseColumnDetails subrowData : mappingMap.get(mappingFaimilyName)) {
                    if (keys.contains(subrowData.getName().getBytes())) {
                        if (subrowData.getType().equals(ParameterNaming.TYPE_TEXT)) {
                            subrowArray[j] = Bytes.toString(familyMap.get(subrowData.getName().getBytes()));
                        } else if (subrowData.getType().equals(ParameterNaming.TYPE_INTEGER)) {
                            subrowArray[j] = Integer
                                    .valueOf(Bytes.toInt(familyMap.get(subrowData.getName().getBytes())));
                        } else if (subrowData.getType().equals(ParameterNaming.TYPE_LONG)) {
                            subrowArray[j] = Long.valueOf(Bytes.toLong(familyMap.get(subrowData.getName().getBytes())));
                        } else if (subrowData.getType().equals(ParameterNaming.TYPE_FLOAT)) {
                            subrowArray[j] = Float
                                    .valueOf(Bytes.toFloat(familyMap.get(subrowData.getName().getBytes())));
                        } else if (subrowData.getType().equals(ParameterNaming.TYPE_DOUBLE)) {
                            subrowArray[j] = Double.valueOf(Bytes.toDouble(familyMap.get(subrowData.getName()
                                    .getBytes())));
                        } else {
                            subrowArray[j] = familyMap.get(subrowData.getName().getBytes());
                        }
                    } else {
                        subrowArray[j] = null;
                    }
                    j++;
                }
                rowArray[i] = subrowArray;
            } else {
                rowArray[i] = null;
            }
            i++;
        }
        // the row key for this row
        rowArray[i] = Bytes.toString(resultSet.getRow());
        return rowArray;
    }

    @Override
    public boolean stop() {
        this.stopRequested = true;
        return this.stopRequested;
    }

}
