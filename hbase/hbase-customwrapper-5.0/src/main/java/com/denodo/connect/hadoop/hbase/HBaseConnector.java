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
import java.io.UnsupportedEncodingException;
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
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

public class HBaseConnector extends AbstractCustomWrapper {

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return new CustomWrapperInputParameter[] {
                new CustomWrapperInputParameter(
                        ParameterNaming.CONF_HBASE_IP,
                        "IP through which we want to access the database system (this parameter can be a list of HBase IPs separated by commas)",
                        true, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(ParameterNaming.CONF_HBASE_PORT,
                        "The ZooKeeper port, an optional field with a default value of 2181",
                        false, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(ParameterNaming.CONF_TABLE_NAME,
                        "HBase table",
                        true, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(ParameterNaming.CONF_TABLE_MAPPING,
                        "Fragment of JSON, giving information about the queried HBase data structure",
                        true, CustomWrapperInputParameterTypeFactory.longStringType()),
                new CustomWrapperInputParameter(ParameterNaming.CONF_CACHING_SIZE,
                        "Number of rows for caching that will be passed to scanners",
                        false, CustomWrapperInputParameterTypeFactory.integerType())

        };
    }

    private boolean stopRequested = false;
    private int filterNumber = 0;

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
                CustomWrapperCondition.OPERATOR_LIKE, CustomWrapperCondition.OPERATOR_ISCONTAINED
                // CustomWrapperCondition.OPERATOR_GT, CustomWrapperCondition.OPERATOR_LT,
                // CustomWrapperCondition.OPERATOR_GE, CustomWrapperCondition.OPERATOR_LE
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
                final Get get = new Get(getBytesFromExpresion(simpleExpression));
                final String logString = buildStringGetQuery(tableName, mappingMap, value);
                log(LOG_TRACE, "In the hbase shell would be:" + logString);
                getCustomWrapperPlan().addPlanEntry("In the hbase shell would be ",
                        logString);
                final Result resultRow = table.get(get);
                if ((resultRow != null) && !resultRow.isEmpty()) {
                    final Object[] rowArray = processRow(resultRow, mappingMap, families);
                    result.addRow(rowArray, HbaseUtil.getGenericOutputpStructure(mappingMap));
                }

            } else {
                if ((conditionComplex != null)) {
                    /*
                     * CACHING, AND OTHER COMPLEX HBASE SCANNING FEATURES COULD BE ADDED HERE
                     */
                    for (final String family : mappingMap.keySet()) {
                        for (final HBaseColumnDetails subrowData : mappingMap.get(family)) {
                            scan.addColumn(family.getBytes(), subrowData.getName().getBytes());
                        }
                    }
                    if (conditionComplex.isAndCondition() || conditionComplex.isOrCondition()) {
                        log(LOG_TRACE,
                                "This query has more than one condition. The filters that appear would be the equivalent "
                                        +
                                        "each one by separate in hbase shell, but not jointly.");
                        getCustomWrapperPlan()
                                .addPlanEntry(
                                        "This query has more than one condition.The filters that appear would be the equivalent "
                                                +
                                                "each one by separate in hbase shell, but not jointly.", "");
                    }
                    final Filter filter = buildFilterFromCustomWrapperCondition(conditionComplex, false, scan,
                            tableName, mappingMap);
                    if (filter != null) {

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
                    final String logString = buildStringScanQueryWithoutConditions(tableName, mappingMap);
                    log(LOG_TRACE, "In the hbase shell would be :  " + logString);
                    getCustomWrapperPlan().addPlanEntry("In the hbase shell would be ", logString);
                }

                final Date date = new Date();
                log(LOG_TRACE, "Time before scan: " + (date.getTime()));
                final ResultScanner scanner = table.getScanner(scan);
                final Date date2 = new Date();
                log(LOG_TRACE, "Time after scan startStop: " + (date2.getTime()));
                log(LOG_TRACE, "It has taken " + (date2.getTime() - date.getTime()) + " milliseconds, the filtering.");
                log(LOG_TRACE, "The resultscanner of the table " + tableName + " has been created successfully.");
                try {

                    for (final Result resultRow : scanner) {
                        // Stop the scan if requested from outside
                        if (this.stopRequested) {
                            break;
                        }

                        final Object[] rowArray = processRow(resultRow, mappingMap, families);
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
            throw new CustomWrapperException("Error accessing HBase: " + e.getMessage(), e);
        }
        log(LOG_INFO, "End- Run hbase-customwrapper");

    }

    private Filter buildFilterFromCustomWrapperCondition(final CustomWrapperCondition conditionComplex,
            final boolean not, final Scan scan, final String tableName,
            final Map<String, List<HBaseColumnDetails>> attributesMappingMap) throws CustomWrapperException,
            UnsupportedEncodingException {

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
                final Filter simpleFilter = buildFilterFromCustomWrapperCondition(condition, not, scan, tableName,
                        attributesMappingMap);
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
                filterList.addFilter(buildFilterFromCustomWrapperCondition(condition, not, scan, tableName,
                        attributesMappingMap));

            }
            return filterList;
        } else if (conditionComplex.isNotCondition()) {
            final CustomWrapperNotCondition conditionNot = (CustomWrapperNotCondition) conditionComplex;

            return buildFilterFromCustomWrapperCondition(conditionNot.getCondition(), true, scan, tableName,
                    attributesMappingMap);
        } else {
            final CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) conditionComplex;

            final CustomWrapperFieldExpression conditionExpression = (CustomWrapperFieldExpression) simpleCondition
                    .getField();
            final String familyColumn = conditionExpression.getName();
            final CustomWrapperExpression[] rightExpresion = simpleCondition.getRightExpression();

            String value = "";
            byte[] bytesValue = null;
            if ((rightExpresion != null) && (rightExpresion.length > 0)) {
                value = rightExpresion[0].toString();
                final CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression) rightExpresion[0];
                bytesValue = getBytesFromExpresion(simpleExpression);

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
                            bytesValue));
                    filter = rowfilter;
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, familyColumn,
                            column, ParameterNaming.ROWKEY_FILTER,
                            operator.name(),
                            value, null, null, false, false);
                    log(LOG_DEBUG,
                            "The hbase shell query equivalent should be  "
                                    +
                                    equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",

                            equivalentQuery);
                } else if (familyColumn.equals(ParameterNaming.COL_STARTROW)) {
                    scan.setStartRow(bytesValue);
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, null,
                            null, null,
                            null,
                            null, value, null, false, false);
                    log(LOG_TRACE,
                            "The filter has the following StartRow:" + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);
                } else if (familyColumn.equals(ParameterNaming.COL_STOPROW)) {
                    scan.setStopRow(bytesValue);
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, null,
                            null, null,
                            null,
                            null, null, value, false, false);
                    log(LOG_TRACE,
                            "The filter has the following StopRow:" + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);

                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new BinaryComparator(bytesValue));
                    if (!not) {
                        // If you want that rows, that has a column with value null,be filtered, it is necessary to
                        // enable
                        // FilterIFMissing
                        filterColumn.setFilterIfMissing(true);

                    }
                    filter = filterColumn;
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, familyColumn,
                            column, ParameterNaming.COLUMN_FILTER,
                            operator.name(),
                            value, null, null, false, not ? false : true);
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be : " + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);
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
                            bytesValue));
                    filter = rowfilter;
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, familyColumn,
                            column, ParameterNaming.ROWKEY_FILTER,
                            operator.name(),
                            value, null, null, false, false);
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be :" +
                                    equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);
                } else if (familyColumn.equals(ParameterNaming.COL_STARTROW)
                        || (familyColumn.equals(ParameterNaming.COL_STOPROW))) {
                    throw new CustomWrapperException(
                            "The parameters StartRow and StopRow only supports the operator EQUAL");
                }
                else {

                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new BinaryComparator(bytesValue));
                    if (not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, familyColumn,
                            column, ParameterNaming.COLUMN_FILTER,
                            operator.name(),
                            value, null, null, false, not ? true : false);
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be :"
                                    + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);

                }
            } else if (simpleCondition.getOperator().equals(Operator.REGEXP_LIKE)) {
                CompareOp operator;
                final String valueRegex = HbaseUtil.quotemeta(value);
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new RegexStringComparator(valueRegex));
                    filter = rowfilter;
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, familyColumn,
                            column, ParameterNaming.ROWKEY_FILTER,
                            operator.name(),
                            valueRegex, null, null, true, false);
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be :" + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);

                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new RegexStringComparator(value));
                    if (!not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, familyColumn,
                            column, ParameterNaming.COLUMN_FILTER,
                            operator.name(),
                            valueRegex, null, null, true, not ? false : true);
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be :"
                                    + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);

                }
            } else if (simpleCondition.getOperator().equals(Operator.LIKE)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                final String valueRegex = HbaseUtil.getRegExpformLike(value);
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {

                    final RowFilter rowfilter = new RowFilter(operator, new RegexStringComparator(
                            valueRegex));
                    filter = rowfilter;
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, familyColumn,
                            column, ParameterNaming.ROWKEY_FILTER,
                            operator.name(),
                            valueRegex, null, null,
                            true, false);
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be :"
                                    +
                                    equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);

                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new RegexStringComparator(HbaseUtil.getRegExpformLike(value)));
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, familyColumn,
                            column, ParameterNaming.COLUMN_FILTER,
                            operator.name(),
                            valueRegex, null, null, true, not ? false : true);
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be :"
                                    + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);

                    if (!not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;
                }

            } else if (simpleCondition.getOperator().equals(Operator.IS_NULL)
                    || simpleCondition.getOperator().equals(Operator.IS_NOT_NULL)) {
                value = new String();
                // It compares with a String empty to know if a column is null or not.
                if ((simpleCondition.getOperator().equals(Operator.IS_NULL) && !not)
                        || (simpleCondition.getOperator().equals(Operator.IS_NOT_NULL) && not)) {
                    // filter = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                    // Bytes.toBytes(column),
                    // CompareOp.NOT_EQUAL, new NullComparator());
                    // It could be possible to find another way more optimal than this
                    if (!familyColumn.equals(ParameterNaming.COL_ROWKEY)) {

                        final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                                Bytes.toBytes(familyColumn),
                                Bytes.toBytes(column),
                                CompareOp.NOT_EQUAL, new RegexStringComparator(value));
                        filter = filterColumn;
                        final String equivalentQuery = buildEquivalentShellQuery(tableName,
                                attributesMappingMap, familyColumn,
                                column, ParameterNaming.COLUMN_FILTER,
                                CompareOp.NOT_EQUAL.name(),
                                value, null, null, true, false);
                        log(LOG_DEBUG,
                                "The hbase shell query equivalent should be : "
                                        +
                                        equivalentQuery);
                        getCustomWrapperPlan().addPlanEntry(
                                "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                                equivalentQuery);
                    } else {
                        final RowFilter rowfilter = new RowFilter(CompareOp.NOT_EQUAL, new RegexStringComparator(value));
                        filter = rowfilter;
                    }
                } else {
                    // final QualifierFilter columnFilter = new QualifierFilter(CompareOp.NO_OP, new BinaryComparator(
                    // Bytes.toBytes(column)));
                    // It could be possible to find another way more optimal than this
                    if (!familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                        final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                                Bytes.toBytes(familyColumn),
                                Bytes.toBytes(column),
                                CompareOp.EQUAL, new RegexStringComparator(value));
                        filterColumn.setFilterIfMissing(true);
                        filter = filterColumn;

                        final String equivalentQuery = buildEquivalentShellQuery(tableName,
                                attributesMappingMap, familyColumn,
                                column, ParameterNaming.COLUMN_FILTER,
                                CompareOp.EQUAL.name(),
                                value, null, null, true, true);
                        log(LOG_DEBUG,
                                "The hbase shell query equivalent should be : "
                                        +
                                        equivalentQuery);
                        getCustomWrapperPlan().addPlanEntry(
                                "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                                equivalentQuery);
                    }
                }

            } else if (simpleCondition.getOperator().equals(Operator.IN)) {
                FilterList.Operator operator;
                CompareOp compareOp;
                log(LOG_TRACE,
                        "Start filters IN ");
                getCustomWrapperPlan().addPlanEntry("Filter IN (" + (this.filterNumber) + ") ",
                        " start");
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
                    int i = 0;
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        if (isRowFilter) {
                            final RowFilter rowfilter = new RowFilter(compareOp, new BinaryComparator(
                                    getBytesFromExpresion((CustomWrapperSimpleExpression) factor)));
                            filter = rowfilter;
                            filterList.addFilter(rowfilter);
                            final String equivalentQuery = buildEquivalentShellQuery(tableName,
                                    attributesMappingMap, familyColumn,
                                    column, ParameterNaming.ROWKEY_FILTER,
                                    operator.name(),
                                    value, null, null, false, false);
                            log(LOG_DEBUG,
                                    "The hbase shell query equivalent should be : "
                                            +
                                            equivalentQuery);
                            getCustomWrapperPlan().addPlanEntry(
                                    "[IN]Simple filter number " + (i++)
                                            + "  (hbase shell query equivalent) ",
                                    equivalentQuery);
                        } else {
                            final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                                    Bytes.toBytes(familyColumn),
                                    Bytes.toBytes(column),
                                    compareOp, new BinaryComparator(
                                            getBytesFromExpresion((CustomWrapperSimpleExpression) factor)));
                            filterColumn.setFilterIfMissing(true);
                            if (!not) {
                                filterColumn.setFilterIfMissing(true);
                            }
                            filterList.addFilter(filterColumn);
                            final String equivalentQuery = buildEquivalentShellQuery(tableName,
                                    attributesMappingMap, familyColumn,
                                    column, ParameterNaming.COLUMN_FILTER,
                                    operator.name(),
                                    value, null, null, false, not ? false : true);
                            log(LOG_DEBUG,
                                    "The hbase shell query equivalent should be : "
                                            +
                                            equivalentQuery);
                            getCustomWrapperPlan().addPlanEntry(
                                    "[IN] Simple filter number " + (i++)
                                            + "  (hbase shell query equivalent) ",
                                    equivalentQuery);
                        }
                    }
                }
                filter = filterList;
                log(LOG_TRACE,
                        "END filters IN ");
                getCustomWrapperPlan().addPlanEntry("Filter IN (" + (this.filterNumber++) + ")_",
                        " end");
            } else if (simpleCondition.getOperator().equals(Operator.CONTAINS)) {
                CompareOp operator;
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                final String valueRegex = HbaseUtil.quotemeta(value);
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {

                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, null,
                            null, ParameterNaming.COL_ROWKEY,
                            operator.name(),
                            valueRegex, null, null, false, false);

                    final RowFilter rowfilter = new RowFilter(operator, new RegexStringComparator(valueRegex));
                    filter = rowfilter;
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be :" + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);
                } else {
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, familyColumn,
                            column, ParameterNaming.COLUMN_FILTER,
                            operator.name(),
                            valueRegex, null, null, true, not ? false : true);
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be :"
                                    + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new RegexStringComparator(valueRegex));
                    if (!not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;
                }
            } else if (simpleCondition.getOperator().equals(Operator.IS_CONTAINED)) {
                CompareOp operator;
                final String valueRegex = HbaseUtil.quotemeta(value);
                if (!not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new RegexStringComparator(valueRegex));
                    filter = rowfilter;
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new RegexStringComparator(valueRegex));
                    if (!not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, null,
                            null, ParameterNaming.COL_ROWKEY,
                            operator.name(),
                            valueRegex, null, null, false, not ? false : true);
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be " + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);
                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                            Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column),
                            operator, new RegexStringComparator(valueRegex));
                    if (!not) {
                        filterColumn.setFilterIfMissing(true);
                    }
                    filter = filterColumn;
                    final String equivalentQuery = buildEquivalentShellQuery(tableName,
                            attributesMappingMap, familyColumn,
                            column, ParameterNaming.COLUMN_FILTER,
                            operator.name(),
                            valueRegex, null, null, true, not ? false : true);
                    log(LOG_TRACE,
                            "The hbase shell query equivalent should be "
                                    + equivalentQuery);
                    getCustomWrapperPlan().addPlanEntry(
                            "Simple filter number " + (this.filterNumber++) + "  (hbase shell query equivalent) ",
                            equivalentQuery);
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
                log(LOG_TRACE,
                        "Start filters CONTAINS_AND ");
                getCustomWrapperPlan().addPlanEntry("Filter CONTAINS_AND (" + this.filterNumber + ")",
                        "start");
                final FilterList filterList = new FilterList(operator);
                if (rightExpresion != null) {
                    int i = 0;
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        final String regexFactor = HbaseUtil.quotemeta(factor.toString());
                        if (isRowFilter) {
                            final RowFilter rowfilter = new RowFilter(compareOp, new RegexStringComparator(
                                    regexFactor));
                            filter = rowfilter;
                            filterList.addFilter(rowfilter);
                            final String equivalentQuery = buildEquivalentShellQuery(tableName,
                                    attributesMappingMap, null,
                                    null, ParameterNaming.ROWKEY_FILTER,
                                    operator.name(),
                                    regexFactor, null, null, true, false);
                            log(LOG_TRACE,
                                    "The hbase shell query equivalent should be "
                                            + equivalentQuery);
                            getCustomWrapperPlan().addPlanEntry(
                                    "[CONTAINS_AND] Simple filter number " + (i++)
                                            + "  (hbase shell query equivalent) ",
                                    equivalentQuery);
                        } else {
                            final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                                    Bytes.toBytes(familyColumn),
                                    Bytes.toBytes(column),
                                    compareOp, new RegexStringComparator(regexFactor));
                            if (!not) {
                                filterColumn.setFilterIfMissing(true);
                            }
                            filterList.addFilter(filterColumn);
                            final String equivalentQuery = buildEquivalentShellQuery(tableName,
                                    attributesMappingMap, familyColumn,
                                    column, ParameterNaming.COLUMN_FILTER,
                                    operator.name(),
                                    regexFactor, null, null, true, not ? false : true);
                            log(LOG_TRACE,
                                    "The hbase shell query equivalent should be :"
                                            + equivalentQuery);
                            getCustomWrapperPlan().addPlanEntry(
                                    "[CONTAINS AND]Simple filter number " + (this.filterNumber++)
                                            + "  (hbase shell query equivalent) ",
                                    equivalentQuery);
                        }
                    }
                }
                log(LOG_TRACE,
                        "End filters CONTAINS_AND ");

                getCustomWrapperPlan().addPlanEntry("Filter CONTAINS_AND (" + (this.filterNumber++) + ")_",
                        "end");
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
                log(LOG_TRACE,
                        "Start filters CONTAINS_OR ");
                getCustomWrapperPlan().addPlanEntry("Filter CONTAINS_OR (" + (this.filterNumber) + ")",
                        "start");

                if (rightExpresion != null) {
                    int i = 0;
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        final String regexFactor = HbaseUtil.quotemeta(factor.toString());
                        if (isRowFilter) {
                            final RowFilter rowfilter = new RowFilter(compareOp, new RegexStringComparator(
                                    factor.toString()));
                            filter = rowfilter;
                            filterList.addFilter(rowfilter);
                            final String equivalentQuery = buildEquivalentShellQuery(tableName,
                                    attributesMappingMap, null,
                                    null, ParameterNaming.ROWKEY_FILTER,
                                    operator.name(),
                                    regexFactor, null, null, true, false);
                            log(LOG_TRACE,
                                    "The hbase shell query equivalent should be :"
                                            + equivalentQuery);
                            getCustomWrapperPlan().addPlanEntry(
                                    "[CONTAINS_OR]Simple filter number " + (i++)
                                            + "  (hbase shell query equivalent) ",
                                    equivalentQuery);
                        } else {
                            final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(
                                    Bytes.toBytes(familyColumn),
                                    Bytes.toBytes(column),
                                    compareOp, new RegexStringComparator(factor.toString()));
                            if (!not) {
                                filterColumn.setFilterIfMissing(true);
                            }
                            filterList.addFilter(filterColumn);
                            final String equivalentQuery = buildEquivalentShellQuery(tableName,
                                    attributesMappingMap, familyColumn,
                                    column, ParameterNaming.COLUMN_FILTER,
                                    operator.name(),
                                    regexFactor, null, null, true, not ? false : true);
                            log(LOG_TRACE,
                                    "The hbase shell query equivalent should be :"
                                            + equivalentQuery);
                            getCustomWrapperPlan().addPlanEntry(
                                    "[CONTAINS_OR]Simple filter number " + (this.filterNumber++)
                                            + "  (hbase shell query equivalent) ",
                                    equivalentQuery);
                        }
                    }
                }
                log(LOG_TRACE,
                        "End filters CONTAINS_OR ");
                getCustomWrapperPlan().addPlanEntry("Filter CONTAINS_OR_(" + (this.filterNumber++) + ")_",
                        "end");
                filter = filterList;
            }

            return filter;
        }
    }

    private static Object[] processRow(final Result resultSet, final Map<String, List<HBaseColumnDetails>> mappingMap,
            final Set<byte[]> families) {

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
                            byte[] content = familyMap.get(subrowData.getName().getBytes());
                            final int max_int = Integer.SIZE / Byte.SIZE;
                            if (content.length < max_int) {
                                content = HbaseUtil.fillWithZeroBytes(content, max_int - content.length);
                            }

                            subrowArray[j] = Integer
                                    .valueOf(Bytes.toInt(content));
                        } else if (subrowData.getType().equals(ParameterNaming.TYPE_LONG)) {
                            byte[] content = familyMap.get(subrowData.getName().getBytes());
                            final int max_long = Long.SIZE / Byte.SIZE;
                            if (content.length < max_long) {
                                content = HbaseUtil.fillWithZeroBytes(content, max_long - content.length);
                            }

                            subrowArray[j] = Long.valueOf(Bytes.toLong(content));

                        } else if (subrowData.getType().equals(ParameterNaming.TYPE_FLOAT)) {
                            byte[] content = familyMap.get(subrowData.getName().getBytes());
                            final int max_float = Float.SIZE / Byte.SIZE;
                            if (content.length < max_float) {
                                content = HbaseUtil.fillWithZeroBytes(content, max_float - content.length);
                            }
                            subrowArray[j] = Float
                                    .valueOf(Bytes.toFloat(content));

                        } else if (subrowData.getType().equals(ParameterNaming.TYPE_DOUBLE)) {
                            byte[] content = familyMap.get(subrowData.getName().getBytes());
                            final int max_long = Long.SIZE / Byte.SIZE;
                            if (content.length < max_long) {
                                content = HbaseUtil.fillWithZeroBytes(content, max_long - content.length);
                            }
                            subrowArray[j] = Double.valueOf(Bytes.toDouble(content));

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

    private static String buildEquivalentShellQuery(final String tableName,
            final Map<String, List<HBaseColumnDetails>> fields,
            final String familyColumn, final String column, final String filter, final String operator,
            final String value, final String startRow, final String stopRow, final Boolean isRegex,
            final Boolean filterIfMissing) {

        final StringBuilder query = new StringBuilder();
        query.append("Before executing this query in the shell, You should import the folowing classes:\n");
        query.append("import org.apache.hadoop.hbase.filter.CompareFilter \n");
        if (ParameterNaming.COLUMN_FILTER.equals(filter)) {
            query.append("import org.apache.hadoop.hbase.filter.SingleColumnValueFilter \n");
        } else {
            query.append("import org.apache.hadoop.hbase.filter.RowFilter \n");
        }

        if (!isRegex) {
            query.append("import org.apache.hadoop.hbase.filter.BinaryComparator \n");
        } else {
            query.append("import org.apache.hadoop.hbase.filter.RegexStringComparator \n");
        }
        if (filter != null) {

            query.append("import org.apache.hadoop.hbase.util.Bytes \n");
            query.append("filter = ").append(filter).append(".new(");
            if (filter.equals(ParameterNaming.COLUMN_FILTER)) {
                query.append("Bytes.toBytes('" + familyColumn).append("'),");
                query.append("Bytes.toBytes('").append(column).append("'),");
            }
            query.append("CompareFilter::CompareOp.valueOf('").append(operator).append("'), ");
            if (isRegex) {
                query.append("RegexStringComparator.new('").append(value).append("'");
            } else {
                query.append("BinaryComparator.new(Bytes.toBytes('").append(value).append("')");
            }
            query.append(")) \n");
            if (filterIfMissing) {
                query.append("filter.setFilterIfMissing(true) \n");
            }
        }
        query.append("scan ");
        query.append("'").append(tableName).append("',{ ");

        query.append("COLUMNS => [");

        for (final String family : fields.keySet()) {
            for (final HBaseColumnDetails subrowData : fields.get(family)) {
                query.append("'").append(family).append(":").append(subrowData.getName())
                        .append("',");
            }
        }

        query.deleteCharAt(query.length() - 1);
        query.append("] ");
        if (startRow != null) {
            query.append(",");
            query.append("STARTROW => '").append(startRow).append("' ");
        }
        if (stopRow != null) {
            query.append(",");
            query.append("STOPROW => '").append(stopRow).append("' ");
        }

        if (filter != null) {
            query.append(",");

            query.append("FILTER => filter ");
        }
        query.append("}");
        query.append("\n");

        return query.toString();

    }

    private static String buildStringGetQuery(final String tableName,
            final Map<String, List<HBaseColumnDetails>> fields,
            final String value) {
        final StringBuilder logString = new StringBuilder();
        logString.append("get  '" + tableName + "','" + value + "',");
        logString.append("{COLUMNS => [");

        for (final String family : fields.keySet()) {
            for (final HBaseColumnDetails subrowData : fields.get(family)) {
                logString.append("'").append(family).append(":").append(subrowData.getName())
                        .append("',");
            }
        }
        logString.deleteCharAt(logString.length() - 1);
        logString.append("]}");

        return logString.toString();
    }

    private static String buildStringScanQueryWithoutConditions(final String tableName,
            final Map<String, List<HBaseColumnDetails>> fields) {
        final StringBuilder logString = new StringBuilder();
        logString.append("scan  '" + tableName + "',");
        logString.append("{COLUMNS => [");

        for (final String family : fields.keySet()) {
            for (final HBaseColumnDetails subrowData : fields.get(family)) {
                logString.append("'").append(family).append(":").append(subrowData.getName())
                        .append("',");
            }
        }
        logString.deleteCharAt(logString.length() - 1);
        logString.append("]}");

        return logString.toString();
    }

    @Override
    public boolean stop() {
        this.stopRequested = true;
        return this.stopRequested;
    }

    public static byte[] getBytesFromExpresion(final CustomWrapperSimpleExpression expression) {
        byte[] value;

        if (expression.getValue() instanceof Integer) {
            value = Bytes.toBytes((Integer) expression.getValue());
        } else if (expression.getValue() instanceof Long) {
            value = Bytes.toBytes((Long) expression.getValue());
        } else if (expression.getValue() instanceof Double) {
            value = Bytes.toBytes((Double) expression.getValue());
        } else if (expression.getValue() instanceof Float) {
            value = Bytes.toBytes((Float) expression.getValue());
        } else if (expression.getValue() instanceof String) {
            value = Bytes.toBytes((String) expression.getValue());
        } else {
            value = Bytes.toBytes((String) expression.getValue());
        }

        return value;
    }

}
