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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

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
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;

public class HBaseConnector extends AbstractCustomWrapper {

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
                CustomWrapperCondition.OPERATOR_GT, CustomWrapperCondition.OPERATOR_LT,
                CustomWrapperCondition.OPERATOR_GE, CustomWrapperCondition.OPERATOR_LE
        });

        return configuration;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(
            final Map<String, String> inputValues) throws CustomWrapperException {

        final String mapping = inputValues.get(ParameterNaming.CONF_TABLE_MAPPING);
        try {

            log(LOG_INFO, "Start getSchemaParameters hbase");
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

            rows.add(new CustomWrapperSchemaParameter(ParameterNaming.COL_STARTROW, java.sql.Types.VARCHAR, null,
                    true, CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false));
            rows.add(new CustomWrapperSchemaParameter(ParameterNaming.COL_STOPROW, java.sql.Types.VARCHAR, null,
                    true, CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false));
            return rows.toArray(new CustomWrapperSchemaParameter[] {});

        } catch (final Exception e) {
            log(LOG_ERROR, "Error in mapping format: " + ExceptionUtils.getStackTrace(e));
            throw new CustomWrapperException("Error in mapping format: ", e);
        }

    }

    @Override
    public void run(final CustomWrapperConditionHolder condition,
            final List<CustomWrapperFieldExpression> projectedFields,
            final CustomWrapperResult result, final Map<String, String> inputValues)
            throws CustomWrapperException {

        log(LOG_INFO, "Start run hbase-customwrapper");
        Map<String, List<HBaseColumnDetails>> mappingMap;
        try {
            final String mapping = inputValues.get(ParameterNaming.CONF_TABLE_MAPPING);
            mappingMap = HbaseUtil.parseMapping(mapping);
        } catch (final Exception e) {
            log(LOG_ERROR, "Error in mapping format: " + ExceptionUtils.getStackTrace(e));
            throw new CustomWrapperException("Error in mapping format: ", e);
        }

        // Connects to HBase server
        final String tableName = inputValues.get(ParameterNaming.CONF_TABLE_NAME);
        final Configuration config = HBaseConfiguration.create();
        config.set(ParameterNaming.CONF_ZOOKEEPER_QUORUM, inputValues.get(ParameterNaming.CONF_HBASE_IP));
        final String port = inputValues.get(ParameterNaming.CONF_HBASE_PORT);
        if (port != null) {
            config.set(ParameterNaming.CONF_ZOOKEEPER_CLIENTPORT, port);
        }
        try {
            HBaseAdmin.checkHBaseAvailable(config);
        } catch (final MasterNotRunningException e) {
            log(LOG_ERROR, "Error Master Hbase not Running: " + ExceptionUtils.getStackTrace(e));
            throw new CustomWrapperException("Error Master Hbase not Running: " + e.getMessage(), e);
        } catch (final ZooKeeperConnectionException e) {
            log(LOG_ERROR, "Error ZooKeeper Connection: " + ExceptionUtils.getStackTrace(e));
            throw new CustomWrapperException("Error ZooKeeper Connection: " + e.getMessage(), e);
        }
        HTable table;

        try {
            // Get table metadata
            table = new HTable(config, tableName);
            log(LOG_TRACE, "the connection was successfully established with HBase");

            final Set<byte[]> families = table.getTableDescriptor().getFamiliesKeys();

            final CustomWrapperCondition conditionComplex = condition.getComplexCondition();
            // Bytes.toBytes("1000000006|1|XA|A"), Bytes.toBytes("1000000008|1|XA|A")
            final Scan scan = new Scan();
            CustomWrapperSimpleCondition simpleCondition = null;
            if (conditionComplex != null) {
                if (condition.getComplexCondition().isSimpleCondition()) {
                    simpleCondition = (CustomWrapperSimpleCondition) conditionComplex;
                }
            }
            if ((simpleCondition != null)
                    && simpleCondition.getField().toString().equals(ParameterNaming.COL_ROWKEY)
                    && (simpleCondition.getOperator().equals(Operator.EQUALS_TO))) {
                // The simple queries by row query are implemented in a different way using Get instead of Scan.
                // Get operates directly on a particular row identified by the rowkey passed as a parameter to the the
                // Get instance.
                // While Scan operates on all the rows, if you haven't used range query by providing start and end
                // rowkeys to your Scan instance.
                final CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression) simpleCondition
                        .getRightExpression()[0];
                final String value = simpleExpression.getValue().toString();
                final Get get = new Get(value.getBytes());
                log(LOG_TRACE, "This filter is by rowkey:   get  " + tableName + "," + value);

                final Result resultRow = table.get(get);
                if ((resultRow != null) && !resultRow.isEmpty()) {
                    final Object[] rowArray = processRow(resultRow, mappingMap, families, null, null);
                    result.addRow(rowArray, HbaseUtil.getGenericOutputpStructure(mappingMap));
                }

            } else {
                if ((conditionComplex != null)) {
                    /*
                     * COMPLEX FILTER DELEGATION, CACHING, AND OTHER COMPLEX HBASE SCANNING FEATURES COULD BE ADDED HERE
                     */
                    for (final String family : mappingMap.keySet()) {
                        for (final HBaseColumnDetails subrowData : mappingMap.get(family)) {
                            scan.addColumn(family.getBytes(), subrowData.getName().getBytes());
                        }
                    }
                    final Filter filter = buildFilterFromCustomWrapperConditionn(conditionComplex, false, scan);
                    if (filter != null) {
                        log(LOG_TRACE, "The complex filter(It does not  show the regexp:    " + filter.toString());
                        scan.setFilter(filter);
                    }

                } else {
                    // For performance reasons, just add to the scanner the families and qualifiers
                    // specified in the mapping
                    for (final String family : mappingMap.keySet()) {
                        for (final HBaseColumnDetails subrowData : mappingMap.get(family)) {
                            scan.addColumn(family.getBytes(), subrowData.getName().getBytes());
                        }
                    }

                    log(LOG_TRACE, "There is not filter in the query ");

                }

                final Date date = new Date();
                log(LOG_TRACE, "Time before scan:" + (date.getTime()));
                final ResultScanner scanner = table.getScanner(scan);
                final Date date2 = new Date();
                log(LOG_TRACE, "Time after scan  startStop" + (date2.getTime()));
                log(LOG_TRACE, "difference" + (date2.getTime() - date.getTime()));
                log(LOG_TRACE, "The resultscanner of the table " + tableName + " has been created successfully.");
                try {
                    final byte[] startRow = scan.getStartRow();
                    final byte[] stopRow = scan.getStopRow();
                    for (final Result resultRow : scanner) {
                        // Stop the scan if requested from outside
                        if (this.stopRequested) {
                            break;
                        }
                        log(LOG_TRACE,
                                "Obtaining the tuple with the following key row :  "
                                        + Bytes.toString(resultRow.getRow()));
                        // add the row to the output
                        final Object[] rowArray = processRow(resultRow, mappingMap, families, startRow,
                                stopRow);
                        result.addRow(rowArray, HbaseUtil.getGenericOutputpStructure(mappingMap));
                    }

                } finally {
                    scanner.close();
                }

                log(LOG_TRACE, "The table " + tableName + " has been scanned successfully.");

            }
        } catch (final IOException ioe) {
            log(LOG_ERROR, "Error relate to IOExpection: " + ExceptionUtils.getStackTrace(ioe));
            throw new CustomWrapperException("Error related to IOExpection: " + ioe.getMessage(), ioe);

        } catch (final Exception e) {
            log(LOG_ERROR, "Error accessing HBase: " + ExceptionUtils.getStackTrace(e));
            throw new CustomWrapperException("Error accessing the HBase table: " + e.getMessage(), e);
        }
        log(LOG_INFO, "End- Run hbase-customwrapper");

    }

    private Filter buildFilterFromCustomWrapperConditionn(final CustomWrapperCondition conditionComplex,
            final boolean not, final Scan scan) throws CustomWrapperException {
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
                final Filter simpleFilter = buildFilterFromCustomWrapperConditionn(condition, not, scan);
                if (simpleFilter != null) {
                    filterList.addFilter(simpleFilter);
                }
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
                filterList.addFilter(buildFilterFromCustomWrapperConditionn(condition, not, scan));

            }
            return filterList;
        } else if (conditionComplex.isNotCondition()) {
            final CustomWrapperNotCondition conditionNot = (CustomWrapperNotCondition) conditionComplex;

            return buildFilterFromCustomWrapperConditionn(conditionNot.getCondition(), true, scan);
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
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new BinaryComparator(
                            Bytes.toBytes(value)));
                    filter = rowfilter;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(RowFilter("
                                    +
                                    ((operator.equals(CompareOp.EQUAL)) ? "=" : "!=") + ", '"
                                    + value + "'))\"}");

                } else if (familyColumn.equals(ParameterNaming.COL_STARTROW)) {
                    scan.setStartRow(Bytes.toBytes(value));
                    log(LOG_TRACE,
                            "The filter has the following StartRow:" + value);

                } else if (familyColumn.equals(ParameterNaming.COL_STOPROW)) {
                    scan.setStopRow(Bytes.toBytes(value));
                    log(LOG_TRACE,
                            "The filter has the following StopRow:" + value);

                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new BinaryComparator(Bytes.toBytes(value)));
                    if (!not) {
                        // If you want that rows, that has a column with value null,be filtered, it is necessary to
                        // enable
                        // FilterIFMissing
                        filterColumn.setFilterIfMissing(true);

                    }
                    filter = filterColumn;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(SingleColumnValueFilter('"
                                    + familyColumn
                                    + "','"
                                    + column
                                    + "',"
                                    + ((operator.equals(CompareOp.EQUAL)) ? "=" : "!=") + ", '" + value + "'))\"}");

                }

            } else if (simpleCondition.getOperator().equals(Operator.NOT_EQUALS_TO)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.NOT_EQUAL;
                } else {
                    operator = CompareOp.EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new BinaryComparator(
                            Bytes.toBytes(value)));
                    filter = rowfilter;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(RowFilter("
                                    +
                                    ((operator.equals(CompareOp.EQUAL)) ? "=" : "!=") + ", '"
                                    + value + "'))\"}");

                } else if (familyColumn.equals(ParameterNaming.COL_STARTROW)
                        || (familyColumn.equals(ParameterNaming.COL_STOPROW))) {
                    throw new CustomWrapperException(
                            "The parameters StartRow and StopRow only supports the operator EQUAL");
                }
                else {

                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new BinaryComparator(Bytes.toBytes(value)));
                    if (not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(SingleColumnValueFilter('"
                                    + familyColumn
                                    + "','"
                                    + column
                                    + "',"
                                    + ((operator.equals(CompareOp.EQUAL)) ? "=" : "!=") + ", '"
                                    + value + "'))\"}");

                }
            } else if (simpleCondition.getOperator().equals(Operator.REGEXP_LIKE)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new RegexStringComparator(value));
                    filter = rowfilter;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(RowFilter("
                                    +
                                    ((operator.equals(CompareOp.EQUAL)) ? "=" : "!=") + ", 'regexstring:"
                                    + value + "'))\"}");

                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new RegexStringComparator(value));
                    if (!not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(SingleColumnValueFilter('"
                                    + familyColumn
                                    + "','"
                                    + column
                                    + "',"
                                    + ((operator.equals(CompareOp.EQUAL)) ? "=" : "!=") + ", 'regexstring:"
                                    + value + "'))\"}");

                }
            } else if (simpleCondition.getOperator().equals(Operator.LIKE)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new RegexStringComparator(
                            HbaseUtil.getRegExpformLike(value)));
                    filter = rowfilter;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(RowFilter("
                                    +
                                    ((operator.equals(CompareOp.EQUAL)) ? "=" : "!=") + ", 'regexstring:"
                                    + HbaseUtil.getRegExpformLike(value) + "'))\"}");

                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new RegexStringComparator(HbaseUtil.getRegExpformLike(value)));

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(SingleColumnValueFilter('"
                                    + familyColumn
                                    + "','"
                                    + column
                                    + "',"
                                    + ((operator.equals(CompareOp.EQUAL)) ? "=" : "!=") + ", 'regexstring:"
                                    + HbaseUtil.getRegExpformLike(value) + "'))\"}");

                    if (!not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;
                }

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
                boolean isRowFilter = false;
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    isRowFilter = true;
                }
                final FilterList filterList = new FilterList(operator);
                if (rightExpresion != null) {
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        if (isRowFilter) {
                            final RowFilter rowfilter = new RowFilter(compareOp, new BinaryComparator(
                                    Bytes.toBytes(factor.toString())));
                            filter = rowfilter;
                            filterList.addFilter(rowfilter);
                        } else {
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
                }
                filter = filterList;
            } else if (simpleCondition.getOperator().equals(Operator.CONTAINS)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new RegexStringComparator(value));
                    filter = rowfilter;
                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new RegexStringComparator(value));
                    if (!not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;
                }
            } else if (simpleCondition.getOperator().equals(Operator.IS_CONTAINED)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new RegexStringComparator(value));
                    filter = rowfilter;
                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new RegexStringComparator(value));
                    if (!not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;
                }
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
                boolean isRowFilter = false;
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    isRowFilter = true;
                }
                final FilterList filterList = new FilterList(operator);
                if (rightExpresion != null) {
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        if (isRowFilter) {
                            final RowFilter rowfilter = new RowFilter(compareOp, new RegexStringComparator(
                                    factor.toString()));
                            filter = rowfilter;
                            filterList.addFilter(rowfilter);
                        } else {
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
                boolean isRowFilter = false;
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    isRowFilter = true;
                }
                final FilterList filterList = new FilterList(operator);
                if (rightExpresion != null) {
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        if (isRowFilter) {
                            final RowFilter rowfilter = new RowFilter(compareOp, new RegexStringComparator(
                                    factor.toString()));
                            filter = rowfilter;
                            filterList.addFilter(rowfilter);
                        } else {
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
                }
                filter = filterList;
            } else if (simpleCondition.getOperator().equals(Operator.GREATER_EQUALS_THAN)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.GREATER_OR_EQUAL;
                } else {
                    operator = CompareOp.LESS;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new BinaryComparator(
                            Bytes.toBytes(value)));
                    filter = rowfilter;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(RowFilter("
                                    +
                                    ((operator.equals(CompareOp.GREATER_OR_EQUAL)) ? ">=" : "<") + ", '"
                                    + value + "'))\"}");

                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new BinaryComparator(Bytes.toBytes(value)));
                    if (!not) {
                        // If you want that rows, that has a column with value null,be filtered, it is necessary to
                        // enable
                        // FilterIFMissing
                        filterColumn.setFilterIfMissing(true);

                    }
                    filter = filterColumn;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(SingleColumnValueFilter('"
                                    + familyColumn
                                    + "','"
                                    + column
                                    + "',"
                                    + ((operator.equals(CompareOp.GREATER_OR_EQUAL)) ? ">=" : "<") + ", '" + value
                                    + "'))\"}");

                }
            } else if (simpleCondition.getOperator().equals(Operator.LESS_THAN)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.LESS;
                } else {
                    operator = CompareOp.GREATER_OR_EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new BinaryComparator(
                            Bytes.toBytes(value)));
                    filter = rowfilter;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(RowFilter("
                                    +
                                    ((operator.equals(CompareOp.LESS)) ? "<" : ">=") + ", '"
                                    + value + "'))\"}");

                } else {

                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new BinaryComparator(Bytes.toBytes(value)));
                    if (!not) {
                        // If you want that rows, that has a column with value null,be filtered, it is necessary to
                        // enable
                        // FilterIFMissing
                        filterColumn.setFilterIfMissing(true);

                    }
                    filter = filterColumn;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(SingleColumnValueFilter('"
                                    + familyColumn
                                    + "','"
                                    + column
                                    + "',"
                                    + (!(operator.equals(CompareOp.GREATER_OR_EQUAL)) ? ">=" : "<") + ", '" + value
                                    + "'))\"}");

                }

            } else if (simpleCondition.getOperator().equals(Operator.GREATER_THAN)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.GREATER;
                } else {
                    operator = CompareOp.LESS_OR_EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new BinaryComparator(
                            Bytes.toBytes(value)));
                    filter = rowfilter;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(RowFilter("
                                    +
                                    ((operator.equals(CompareOp.GREATER)) ? ">" : "<=") + ", '"
                                    + value + "'))\"}");

                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new BinaryComparator(Bytes.toBytes(value)));
                    if (!not) {
                        // If you want that rows, that has a column with value null,be filtered, it is necessary to
                        // enable
                        // FilterIFMissing
                        filterColumn.setFilterIfMissing(true);

                    }
                    filter = filterColumn;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(SingleColumnValueFilter('"
                                    + familyColumn
                                    + "','"
                                    + column
                                    + "',"
                                    + ((operator.equals(CompareOp.GREATER)) ? ">" : "<=") + ", '" + value + "'))\"}");

                }
            } else if (simpleCondition.getOperator().equals(Operator.LESS_EQUALS_THAN)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.LESS_OR_EQUAL;
                } else {
                    operator = CompareOp.GREATER;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new BinaryComparator(
                            Bytes.toBytes(value)));
                    filter = rowfilter;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(RowFilter("
                                    +
                                    (!(operator.equals(CompareOp.GREATER)) ? ">" : "<=") + ", '"
                                    + value + "'))\"}");

                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new BinaryComparator(Bytes.toBytes(value)));
                    if (!not) {
                        // If you want that rows, that has a column with value null,be filtered, it is necessary to
                        // enable
                        // FilterIFMissing
                        filterColumn.setFilterIfMissing(true);

                    }
                    filter = filterColumn;

                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : scan 'tablename',{FILTER => \"(SingleColumnValueFilter('"
                                    + familyColumn
                                    + "','"
                                    + column
                                    + "',"
                                    + (!(operator.equals(CompareOp.GREATER)) ? ">" : "<=") + ", '" + value + "'))\"}");

                }
            } else {
                filter = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                        Bytes.toBytes(column),
                        CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
            }

            return filter;
        }
    }

    private static Object[] processRow(final Result resultSet, final Map<String, List<HBaseColumnDetails>> mappingMap,
            final Set<byte[]> families, final byte[] startRow, final byte[] stopRow) {

        // Iterates through the families if they are mapped
        final Object[] rowArray = new Object[mappingMap.keySet().size() + 1];

        int i = 0;
        for (final String mappingFamilyName : mappingMap.keySet()) {

            // the row contains the mapped family
            if (families.contains(mappingFamilyName.getBytes())) {
                final NavigableMap<byte[], byte[]> familyMap = resultSet.getFamilyMap(mappingFamilyName.getBytes());

                final Set<byte[]> keys = familyMap.keySet();
                final Object[] subrowArray = new Object[mappingMap.get(mappingFamilyName).size()];
                int j = 0;
                // And fills the sub-rows
                for (final HBaseColumnDetails subrowData : mappingMap.get(mappingFamilyName)) {
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
        // rowArray[i++] = startRow;
        // rowArray[i++] = stopRow;
        return rowArray;
    }

    @Override
    public boolean stop() {
        this.stopRequested = true;
        return this.stopRequested;
    }

}
