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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.denodo.connect.hadoop.hbase.commons.naming.ParameterNaming;
import com.denodo.connect.hadoop.hbase.util.HbaseUtil;
import com.denodo.connect.hadoop.hdfs.util.krb5.KerberosUtils;
import com.denodo.connect.hadoop.hdfs.wrapper.AbstractSecureHadoopWrapper;
import com.denodo.vdb.catalog.operator.Operator;
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
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterLocalRouteValue;

public class HBaseConnector extends AbstractSecureHadoopWrapper {

    private boolean stopRequested = false;
    private int filterNumber = 0;
    
    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
                new CustomWrapperInputParameter(
                        ParameterNaming.CONF_HBASE_IP,
                        "IP through which we want to access the database system (this parameter can be a list of HBase IPs separated by commas)",
                        true, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(ParameterNaming.CONF_HBASE_PORT,
                        "The ZooKeeper port, an optional field with a default value of 2181",
                        false, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(ParameterNaming.CONF_TABLE_NAME,
                        "HBase table", true, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(ParameterNaming.CONF_TABLE_MAPPING,
                        "Fragment of JSON, giving information about the queried HBase data structure",
                        true, CustomWrapperInputParameterTypeFactory.longStringType()),
                new CustomWrapperInputParameter(ParameterNaming.CONF_CACHING_SIZE,
                        "Number of rows for caching that will be passed to scanners",
                        false, CustomWrapperInputParameterTypeFactory.integerType()),
                new CustomWrapperInputParameter(ParameterNaming.CONF_PATH_CONF,
                                "Local route of hbase configuration file (hbase-site.xml)",
                                false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL}))

        };

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(INPUT_PARAMETERS, super.getInputParameters());
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        final CustomWrapperConfiguration configuration = new CustomWrapperConfiguration();
        configuration.setDelegateProjections(true);
        configuration.setDelegateCompoundFieldProjections(true); // NOTE that "#18209 - Setting property delegateCompoundFieldProjections to true does not have effect"
                                                                 // VDP will postfilter results while this bug is not fixed
        configuration.setDelegateNotConditions(true);
        configuration.setDelegateOrConditions(true);
        configuration.setDelegateRightLiterals(true);

        configuration.setAllowedOperators(new String[] { 
                CustomWrapperCondition.OPERATOR_EQ, CustomWrapperCondition.OPERATOR_NE,
                CustomWrapperCondition.OPERATOR_ISNULL, CustomWrapperCondition.OPERATOR_ISNOTNULL,
                CustomWrapperCondition.OPERATOR_ISTRUE, CustomWrapperCondition.OPERATOR_ISFALSE,
                CustomWrapperCondition.OPERATOR_LIKE, CustomWrapperCondition.OPERATOR_REGEXPLIKE,
                CustomWrapperCondition.OPERATOR_CONTAINSAND, CustomWrapperCondition.OPERATOR_CONTAINSOR,
                CustomWrapperCondition.OPERATOR_IN
        });

        return configuration;
    }

    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(final Map<String, String> inputValues) throws CustomWrapperException {

        try {

            log(LOG_INFO, "Start getSchemaParameters hbase");

            final String mapping = inputValues.get(ParameterNaming.CONF_TABLE_MAPPING);
            final Map<String, List<HBaseColumnDetails>> mappingMap = HbaseUtil.parseMapping(mapping);

            final ArrayList<CustomWrapperSchemaParameter> rows = new ArrayList<CustomWrapperSchemaParameter>();

            // row key
            rows.add(new CustomWrapperSchemaParameter(ParameterNaming.COL_ROWKEY, java.sql.Types.VARCHAR, null,
                    true, CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false));
            
            // output schema based on the provided json
            for (final String col : mappingMap.keySet()) {
                final ArrayList<CustomWrapperSchemaParameter> subrows = new ArrayList<CustomWrapperSchemaParameter>();
                for (final HBaseColumnDetails subrowData : mappingMap.get(col)) {
                    subrows.add(new CustomWrapperSchemaParameter(subrowData.getName(), HbaseUtil.getSQLType(subrowData.getType()),
                            null, true, CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false));

                }
                rows.add(new CustomWrapperSchemaParameter(col, java.sql.Types.STRUCT, subrows.toArray(new CustomWrapperSchemaParameter[] {})));
            }

            rows.add(new CustomWrapperSchemaParameter(ParameterNaming.COL_STARTROW, java.sql.Types.VARCHAR, null,
                    true, CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false));
            rows.add(new CustomWrapperSchemaParameter(ParameterNaming.COL_STOPROW, java.sql.Types.VARCHAR, null,
                    true, CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false));
            log(LOG_INFO, "End getSchemaParameters hbase");
            return rows.toArray(new CustomWrapperSchemaParameter[] {});

        } catch (final Exception e) {
            log(LOG_ERROR, "Error in table mapping format: " + ExceptionUtils.getStackTrace(e));
            throw new CustomWrapperException("Error in table mapping format: " + e.getMessage(), e);
        }

    }

    @Override
    public void doRun(final CustomWrapperConditionHolder condition, final List<CustomWrapperFieldExpression> projectedFields,
            final CustomWrapperResult result, final Map<String, String> inputValues) throws CustomWrapperException {

        log(LOG_INFO, "Start run hbase-customwrapper");
        String configurationPath = null;
        if (inputValues.get(ParameterNaming.CONF_PATH_CONF) != null) {
            configurationPath = ((CustomWrapperInputParameterLocalRouteValue) getInputParameterValue(ParameterNaming.CONF_PATH_CONF)).getPath();
        }
        final Configuration hbaseConfig = getHBaseConfig(inputValues, configurationPath);
        
      
        /** Connection to the cluster. A single connection shared by all application threads. */
        Connection connection = null;
        /** A lightweight handle to a specific table. Used from a single thread. */
        Table table = null;
        try {
           
            final TableName tableName =  TableName.valueOf(inputValues.get(ParameterNaming.CONF_TABLE_NAME));
            connection = ConnectionFactory.createConnection(hbaseConfig);
            
         
            Admin admin = connection.getAdmin();
            try{
                if (!admin.tableExists(tableName)) {
                    try{
                        log(LOG_ERROR, "Error connecting HBase: The table does not exist or there is a problem in the connection. " );
                        throw new CustomWrapperException("Error connecting HBase: The table does not exist or there is a problem in the connection. ");
                    }finally{
                        admin.close();
                        connection.close();
                    }
                } 

            }catch(RuntimeException e){
                //TODO this handler of this execption is because in the version 1.x of habse the exeception that is thown is a NPE, that is not very descriptive,
                //before hbase trew other kind of execption more descriptive. When hbase fix this bug, we can delete this handler 
                StackTraceElement[] traceStack = e.getStackTrace();
                if(e.getCause().toString().contains("NullPointerException")){
                    for (int i = 0; i < traceStack.length; i++) {

                        if(traceStack[i].toString().contains("t.RpcRetryingCaller.callWithoutRetries")){
                            log(LOG_ERROR, "Error accessing HBase: This error could mean that there is an error in the authentication,though this might not be the only possible cause. Check the value configured in 'zookeeper.znode.parent'. There could be a mismatch with the one configured in the master.   " + ExceptionUtils.getStackTrace(e));
                            throw new CustomWrapperException( e.getMessage()+"\n  This exception could mean that there is a error in the authentication, though this might not be the only possible cause. Check the value configured in 'zookeeper.znode.parent'. There could be a mismatch with the one configured in the master.   " , e);
                        }

                    }
                }else{
                    log(LOG_ERROR, "Error accessing HBase: " + ExceptionUtils.getStackTrace(e));
                    throw new CustomWrapperException("Error accessing HBase: " + e.getMessage(), e);
                }
            }finally{
                admin.close();
            }

            // retrieve a handle to the target table.
            table = connection.getTable(tableName);
            log(LOG_TRACE, "Connection was successfully established with HBase");
            
            final String mapping = inputValues.get(ParameterNaming.CONF_TABLE_MAPPING);
            Map<String, List<HBaseColumnDetails>> mappingMap = HbaseUtil.parseMapping(mapping);
            mappingMap = filterNonProjectedFields(mappingMap, projectedFields);
            
            final CustomWrapperCondition complexCondition = condition.getComplexCondition();
            if (isSingleRowResult(complexCondition)) {
                log(LOG_TRACE, "The query returns a single row. Using the methos GET");
                CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) complexCondition;                
                
                final CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression) simpleCondition.getRightExpression()[0];
                final String value = simpleExpression.getValue().toString();
                final Get get = new Get(getBytesFromExpresion(simpleExpression));
                recordShellGetCommand(tableName.getNameAsString(), mappingMap, value);

                final Result resultRow = table.get(get);
                if (!resultRow.isEmpty()) {
                    log(LOG_TRACE, "The query of hbase has returned "+resultRow.size()+" rows");
                    final Object[] rowArray = processRow(resultRow, mappingMap, projectedFields);
                    result.addRow(rowArray, projectedFields);
                }
            } else {
                log(LOG_TRACE, "The query could be return several rows. Using the methos SCAN");
                Scan scan = new Scan();
                if (inputValues.containsKey(ParameterNaming.CONF_CACHING_SIZE)) {
                    Integer cacheSize = (Integer) getInputParameterValue(ParameterNaming.CONF_CACHING_SIZE).getValue();
                    log(LOG_INFO, "Using cache size of " + cacheSize);
                    scan.setCaching(cacheSize.intValue());
                }
                
                for (final Map.Entry<String, List<HBaseColumnDetails>> entry : mappingMap.entrySet()) {
                    String family = entry.getKey();
                    List<HBaseColumnDetails> columns = entry.getValue();
                    for (final HBaseColumnDetails column : columns) {
                        scan.addColumn(family.getBytes(), column.getName().getBytes());
                    }
                }
                
                Filter rowKeyFilter = null;
                if (mappingMap.isEmpty()) {
                    rowKeyFilter = buildRowKeyFilter(tableName.getNameAsString());
                }
                
                Filter conditionFilter = null;
                if ((complexCondition != null)) {
                    conditionFilter = buildFilterFromCondition(complexCondition, false, scan, tableName.getNameAsString(), mappingMap);                    
                    if (complexCondition.isAndCondition() || complexCondition.isOrCondition()) {
                        log(LOG_TRACE, "This query has more than one condition. The commands listed, each one by separate, would be the equivalent "
                                + "in HBase shell.");
                        getCustomWrapperPlan().addPlanEntry("This query has more than one condition.The commands listed, each one by separate, "
                                + "would be the equivalent in HBase shell.", "");
                    }

                } else if (!mappingMap.isEmpty()) {
                    recordShellScanCommand(tableName.getNameAsString(), mappingMap, null, null);
                }
                
                Filter filter = buildFilter(rowKeyFilter, conditionFilter);
                scan.setFilter(filter);
                
                long startTime = System.nanoTime();
                final ResultScanner scanner = table.getScanner(scan);
              
                long elapsedTime = System.nanoTime() - startTime;
                double milliseconds = elapsedTime / 1000000.0;
                log(LOG_TRACE, "Scanning has taken " + milliseconds  + " milliseconds.");
                log(LOG_TRACE, "The resultscanner of the table " + tableName + " has been created successfully.");
                try {

                    startTime = System.nanoTime();
                    long countRows=0;
                    for (final Result resultRow : scanner) {
                        countRows++;
                        if (this.stopRequested) {
                            break;
                        }

                        final Object[] rowArray = processRow(resultRow, mappingMap, projectedFields);
                        result.addRow(rowArray, projectedFields);
                    }
                    log(LOG_TRACE, "The query of hbase has returned "+countRows+" rows");
                    elapsedTime = System.nanoTime() - startTime;
                    milliseconds = elapsedTime / 1000000.0;
                    log(LOG_TRACE, "Retrieving has taken " + milliseconds  + " milliseconds.");

                } finally {
                    scanner.close();
                }

                log(LOG_TRACE, "The table " + tableName + " has been scanned successfully.");

            }

            log(LOG_INFO, "End- Run hbase-customwrapper");

        } catch (final TableNotFoundException e) {
            log(LOG_ERROR, "Table not found: " + ExceptionUtils.getStackTrace(e));
            throw new CustomWrapperException("Table not found: " + e.getMessage(), e);
        } catch (final Exception e) {
            log(LOG_ERROR, "Error accessing HBase: " + ExceptionUtils.getStackTrace(e));
            throw new CustomWrapperException("Error accessing HBase: " + e.getMessage(), e);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    log(LOG_ERROR, "Error closing HBase table: " + ExceptionUtils.getStackTrace(e));
                }
            }

            if (connection != null)
                try {
                    connection.close();
                } catch (IOException e) {
                    log(LOG_ERROR, "Error closing connection HBase table: " + ExceptionUtils.getStackTrace(e));
                }

        }

    }

    /*
     * NOTE that due to "#18209 - Setting property delegateCompoundFieldProjections to true does not have effect"
     * projectedFields will not contain subfields so this method only deals with families and not with their columns.
     * VDP will postfilter results while this bug is not fixed.
     */
    private static Map<String, List<HBaseColumnDetails>> filterNonProjectedFields(Map<String, List<HBaseColumnDetails>> mappingMap,
            List<CustomWrapperFieldExpression> projectedFields) {

        Map<String, List<HBaseColumnDetails>> projectionMap = new LinkedHashMap<String, List<HBaseColumnDetails>>();
        for (CustomWrapperFieldExpression projectedField : projectedFields) {
            String projectedFieldName = projectedField.getName();
            if (mappingMap.containsKey(projectedFieldName)) {
                projectionMap.put(projectedFieldName, mappingMap.get(projectedFieldName));
            }
        }
        
        return projectionMap;
    }

    private Configuration getHBaseConfig(final Map<String, String> inputValues, String configurationPath) {
        
        final Configuration config = HBaseConfiguration.create();
        if(configurationPath!=null){
            config.addResource(new Path("C:\\Users\\pleira\\hbase-site.xml"));        
        }
        final String hbaseIP = inputValues.get(ParameterNaming.CONF_HBASE_IP);
        config.set(ParameterNaming.CONF_ZOOKEEPER_QUORUM, hbaseIP);

        final String port = inputValues.get(ParameterNaming.CONF_HBASE_PORT);
        if (port != null) {
            config.set(ParameterNaming.CONF_ZOOKEEPER_CLIENTPORT, port);
        }

        if (isSecurityEnabled()) {
            setSecureProperties(config);
        }
        
        return config;
    }

    private void setSecureProperties(final Configuration config) {

        config.set("hbase.security.authentication", "Kerberos");

        if (loginWithKerberosTicket()) {
            
            // NOTE: Although using the Kerberos ticket requested with kinit Hadoop code requires the kerberos principal name that runs the HMaster process.
            // The principal should be in the form: user/hostname@REALM.  If "_HOST" is used as the hostname portion, 
            // it will be replaced with the actual hostname of the running instance. 
            // But the realm is also required and we do not know which realm is, so we use a fake realm: EXAMPLE.COM
            config.set("hbase.master.kerberos.principal", "hbase/_HOST@EXAMPLE.COM");
            config.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@EXAMPLE.COM");
        } else {
            final String serverPrincipal = getHBasePrincipal();
            config.set("hbase.master.kerberos.principal", serverPrincipal);
            config.set("hbase.regionserver.kerberos.principal", serverPrincipal);
        }
    }
    
    private String getHBasePrincipal() {

        final String realm = KerberosUtils.getRealm(getUserPrincipal());
        // If "_HOST" is used as the hostname portion, 
        // it will be replaced with the actual hostname of the running instance. 
        return "hbase/_HOST@" + realm;
    }
    
    private static boolean isSingleRowResult(CustomWrapperCondition condition) {
        
        if (condition != null && condition.isSimpleCondition()) {
            CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) condition;

            return ParameterNaming.COL_ROWKEY.equals(simpleCondition.getField().toString())
                && (Operator.EQUALS_TO.equals(simpleCondition.getOperator()));
        }
        
        return false;
    }

    private Filter buildRowKeyFilter(String tableName) {
        
        final String equivalentQuery = "Before executing this query in the shell you should import the following class:\n"
                + "import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter \nscan '" + tableName
                + "',FILTER=>\"FirstKeyOnlyFilter()\"\n";
        log(LOG_DEBUG, "HBase shell command:  " + equivalentQuery);
        getCustomWrapperPlan().addPlanEntry("HBase shell command " + this.filterNumber++, equivalentQuery);
        
        return new FirstKeyOnlyFilter();
    }
    
    private static Filter buildFilter(Filter rowKeyFilter, Filter conditionFilter) {
        
        if (rowKeyFilter != null || conditionFilter != null) {
            FilterList mergedFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            if (rowKeyFilter != null) {
                mergedFilter.addFilter(rowKeyFilter);
            }
            if (conditionFilter != null) {
                mergedFilter.addFilter(conditionFilter);
            }
            
            return mergedFilter;
        }
        
        return null;
        
    }

    private Filter buildFilterFromCondition(final CustomWrapperCondition conditionComplex, final boolean not, final Scan scan,
            final String tableName, final Map<String, List<HBaseColumnDetails>> mappingMap) throws CustomWrapperException {

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
                final Filter simpleFilter = buildFilterFromCondition(condition, not, scan, tableName, mappingMap);
                if (simpleFilter != null) {
                    filterList.addFilter(simpleFilter);
                }
            }
            return filterList.getFilters().isEmpty() ? null : filterList;
        } else if (conditionComplex.isOrCondition()) {
            FilterList.Operator operator;
            if (not) {
                operator = FilterList.Operator.MUST_PASS_ALL;
            } else {
                operator = FilterList.Operator.MUST_PASS_ONE;
            }
            final FilterList filterList = new FilterList(operator);
            final CustomWrapperOrCondition conditionOr = (CustomWrapperOrCondition) conditionComplex;
            for (final CustomWrapperCondition condition : conditionOr.getConditions()) {
                Filter simpleFilter = buildFilterFromCondition(condition, not, scan, tableName, mappingMap);
                if (simpleFilter != null) {
                    filterList.addFilter(simpleFilter);
                }
                
            }
            return filterList.getFilters().isEmpty() ? null : filterList;
        } else if (conditionComplex.isNotCondition()) {
            final CustomWrapperNotCondition conditionNot = (CustomWrapperNotCondition) conditionComplex;

            return buildFilterFromCondition(conditionNot.getCondition(), true, scan, tableName, mappingMap);
        } else {
            final CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) conditionComplex;

            final CustomWrapperFieldExpression conditionExpression = (CustomWrapperFieldExpression) simpleCondition.getField();
            final String familyColumn = conditionExpression.getName();
            final CustomWrapperExpression[] rightExpresion = simpleCondition.getRightExpression();

            checkConditionSyntax(simpleCondition, familyColumn, not);

            String value = "";
            byte[] bytesValue = null;
            if ((rightExpresion != null) && (rightExpresion.length > 0)) {
                value = rightExpresion[0].toString();
                if (rightExpresion[0].isSimpleExpression()) {
                    final CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression) rightExpresion[0];
                    bytesValue = getBytesFromExpresion(simpleExpression);
                }
            }

            Filter filter = null;
            String column = "";
            if (conditionExpression.hasSubFields()) {
                final List<CustomWrapperFieldExpression> list = conditionExpression.getSubFields();
                column = list.get(0).toString();
            }
            
            if (simpleCondition.getOperator().equals(Operator.EQUALS_TO)) {
                CompareOp operator;
                if (not) {
                    operator = CompareOp.NOT_EQUAL;
                } else {
                    operator = CompareOp.EQUAL;
                }

                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new BinaryComparator(bytesValue));
                    filter = rowfilter;
                    recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.ROWKEY_FILTER,
                            operator.name(), value, false, false);
                    
                } else if (familyColumn.equals(ParameterNaming.COL_STARTROW)) {
                    scan.setStartRow(bytesValue);
                    recordShellScanCommand(tableName, mappingMap, value, null);
                } else if (familyColumn.equals(ParameterNaming.COL_STOPROW)) {
                    scan.setStopRow(bytesValue);
                    recordShellScanCommand(tableName, mappingMap, null, value);

                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column), operator, new BinaryComparator(bytesValue));

                    filterColumn.setFilterIfMissing(!not);
                    filter = filterColumn;
                    recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                            operator.name(), value, false, !not);
                }

            } else if (simpleCondition.getOperator().equals(Operator.NOT_EQUALS_TO)) {
                CompareOp operator;
                if (not) {
                    operator = CompareOp.EQUAL;
                } else {
                    operator = CompareOp.NOT_EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new BinaryComparator(bytesValue));
                    filter = rowfilter;
                    recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.ROWKEY_FILTER,
                            operator.name(), value, false, false);
                } else if (not && familyColumn.equals(ParameterNaming.COL_STARTROW)) {
                    scan.setStartRow(bytesValue);
                    recordShellScanCommand(tableName, mappingMap, value, null);
                } else if (not && familyColumn.equals(ParameterNaming.COL_STOPROW)) {
                    scan.setStopRow(bytesValue);
                    recordShellScanCommand(tableName, mappingMap, null, value);

                } else {

                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column), operator, new BinaryComparator(bytesValue));
                    filterColumn.setFilterIfMissing(not);
                    filter = filterColumn;
                    recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                            operator.name(), value, false, not);
                }
            } else if (simpleCondition.getOperator().equals(Operator.REGEXP_LIKE)) {
                CompareOp operator;
                if (not) {
                    operator = CompareOp.NOT_EQUAL;
                } else {
                    operator = CompareOp.EQUAL;
                }
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    final RowFilter rowfilter = new RowFilter(operator, new RegexStringComparator(value));
                    filter = rowfilter;
                    recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.ROWKEY_FILTER,
                            operator.name(), value, true, false);
                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column), operator, new RegexStringComparator(value));
                    filterColumn.setFilterIfMissing(!not);
                    filter = filterColumn;
                    recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                            operator.name(), value, true, !not);
                }
            } else if (simpleCondition.getOperator().equals(Operator.LIKE)) {
                CompareOp operator;
                if (not) {
                    operator = CompareOp.NOT_EQUAL;
                } else {
                    operator = CompareOp.EQUAL;
                }
                final String valueRegex = HbaseUtil.getRegExpformLike(value);
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {

                    final RowFilter rowfilter = new RowFilter(operator, new RegexStringComparator(valueRegex));
                    filter = rowfilter;
                    recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.ROWKEY_FILTER,
                            operator.name(), valueRegex, true, false);
                } else {
                    final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                            Bytes.toBytes(column), operator, new RegexStringComparator(valueRegex));
                    filterColumn.setFilterIfMissing(!not);
                    filter = filterColumn;
                    recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                            operator.name(), valueRegex, true, !not);
                }

            } else if (simpleCondition.getOperator().equals(Operator.IS_NULL)
                    || simpleCondition.getOperator().equals(Operator.IS_NOT_NULL)) {
                value = "";
                // Compares with an empty string to know if a column is null
                if ((simpleCondition.getOperator().equals(Operator.IS_NULL) && !not)
                        || (simpleCondition.getOperator().equals(Operator.IS_NOT_NULL) && not)) {
                    // It could be possible to find another way more optimal than this
                    if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                        final RowFilter rowfilter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
                        filter = rowfilter;
                        recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.ROWKEY_FILTER,
                                CompareOp.EQUAL.name(), value, false, false);
                    } else {
                        final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                                Bytes.toBytes(column), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
                        filter = filterColumn;
                        recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                                CompareOp.EQUAL.name(), value, false, false);
                    }
                } else { // Compares with an empty string to know if a column is not null
                    // It could be possible to find another way more optimal than this
                    if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                        final RowFilter rowfilter = new RowFilter(CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(value)));
                        filter = rowfilter;
                        recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.ROWKEY_FILTER,
                                CompareOp.NOT_EQUAL.name(), value, false, false);
                    } else {
                        final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                                Bytes.toBytes(column), CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(value)));
                        filterColumn.setFilterIfMissing(true);
                        filter = filterColumn;

                        recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                                CompareOp.NOT_EQUAL.name(), value, false, true);
                    }
                }

            } else if (simpleCondition.getOperator().equals(Operator.IN)) {
                FilterList.Operator operator;
                CompareOp compareOp;
                log(LOG_TRACE, "Start filters IN ");
                getCustomWrapperPlan().addPlanEntry("Filter IN (" + (this.filterNumber) + ") ", " start");
                if (not) {
                    operator = FilterList.Operator.MUST_PASS_ALL;
                    compareOp = CompareOp.NOT_EQUAL;
                } else {
                    operator = FilterList.Operator.MUST_PASS_ONE;
                    compareOp = CompareOp.EQUAL;
                }
                boolean isRowFilter = false;
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    isRowFilter = true;
                }
                final FilterList filterList = new FilterList(operator);
                if (rightExpresion != null) {
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        if (factor.isSimpleExpression()) {
                            if (isRowFilter) {
                                final RowFilter rowfilter = new RowFilter(compareOp, new BinaryComparator(
                                        getBytesFromExpresion((CustomWrapperSimpleExpression) factor)));
                                filter = rowfilter;
                                filterList.addFilter(rowfilter);
                                recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.ROWKEY_FILTER,
                                        compareOp.name(), factor.toString(), false, false);
                            } else {
                                final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                                        Bytes.toBytes(column), compareOp, new BinaryComparator(
                                                getBytesFromExpresion((CustomWrapperSimpleExpression) factor)));
                                filterColumn.setFilterIfMissing(!not);
                                filterList.addFilter(filterColumn);
                                recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                                        compareOp.name(), factor.toString(), false, !not);
                            }
                        }
                    }
                }
                filter = filterList;
                log(LOG_TRACE, "END filters IN ");
                getCustomWrapperPlan().addPlanEntry("Filter IN (" + (this.filterNumber++) + ")_", " end");
            } else if (simpleCondition.getOperator().equals(Operator.CONTAINS_AND)) {
                FilterList.Operator operator;
                CompareOp compareOp;
                if (not) {
                    operator = FilterList.Operator.MUST_PASS_ONE;
                    compareOp = CompareOp.NOT_EQUAL;
                } else {
                    operator = FilterList.Operator.MUST_PASS_ALL;
                    compareOp = CompareOp.EQUAL;
                }
                boolean isRowFilter = false;
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    isRowFilter = true;
                }
                log(LOG_TRACE, "Start filters CONTAINS_AND ");
                getCustomWrapperPlan().addPlanEntry("Filter CONTAINS_AND (" + this.filterNumber + ")", "start");
                final FilterList filterList = new FilterList(operator);
                if (rightExpresion != null) {
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        final String regexFactor = HbaseUtil.quotemeta(factor.toString());
                        if (isRowFilter) {
                            final RowFilter rowfilter = new RowFilter(compareOp, new RegexStringComparator(regexFactor));
                            filter = rowfilter;
                            filterList.addFilter(rowfilter);
                            recordShellScanWithFiltersCommand(tableName, mappingMap, null, null, ParameterNaming.ROWKEY_FILTER,
                                    compareOp.name(), regexFactor, true, false);
                        } else {
                            final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                                    Bytes.toBytes(column), compareOp, new RegexStringComparator(regexFactor));
                            filterColumn.setFilterIfMissing(!not);
                            filterList.addFilter(filterColumn);
                            recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                                    compareOp.name(), regexFactor, true, !not);
                        }
                    }
                }
                log(LOG_TRACE, "End filters CONTAINS_AND ");

                getCustomWrapperPlan().addPlanEntry("Filter CONTAINS_AND (" + (this.filterNumber++) + ")_", "end");
                filter = filterList;
            } else if (simpleCondition.getOperator().equals(Operator.CONTAINS_OR)) {
                FilterList.Operator operator;
                CompareOp compareOp;
                if (not) {
                    operator = FilterList.Operator.MUST_PASS_ALL;
                    compareOp = CompareOp.NOT_EQUAL;
                } else {
                    operator = FilterList.Operator.MUST_PASS_ONE;
                    compareOp = CompareOp.EQUAL;
                }
                boolean isRowFilter = false;
                if (familyColumn.equals(ParameterNaming.COL_ROWKEY)) {
                    isRowFilter = true;
                }
                final FilterList filterList = new FilterList(operator);
                log(LOG_TRACE, "Start filters CONTAINS_OR ");
                getCustomWrapperPlan().addPlanEntry("Filter CONTAINS_OR (" + (this.filterNumber) + ")", "start");

                if (rightExpresion != null) {
                    for (final CustomWrapperExpression factor : rightExpresion) {
                        final String regexFactor = HbaseUtil.quotemeta(factor.toString());
                        if (isRowFilter) {
                            final RowFilter rowfilter = new RowFilter(compareOp, new RegexStringComparator(factor.toString()));
                            filter = rowfilter;
                            filterList.addFilter(rowfilter);
                            recordShellScanWithFiltersCommand(tableName, mappingMap, null, null, ParameterNaming.ROWKEY_FILTER,
                                    compareOp.name(), regexFactor, true, false);
                        } else {
                            final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                                    Bytes.toBytes(column), compareOp, new RegexStringComparator(factor.toString()));
                            filterColumn.setFilterIfMissing(!not);
                            filterList.addFilter(filterColumn);
                            recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                                    compareOp.name(), regexFactor, true, !not);
                        }
                    }
                }
                log(LOG_TRACE, "End filters CONTAINS_OR ");
                getCustomWrapperPlan().addPlanEntry("Filter CONTAINS_OR_(" + (this.filterNumber++) + ")_", "end");
                filter = filterList;
            } else if (simpleCondition.getOperator().equals(Operator.IS_TRUE)) {

                CompareOp operator;
                if (not) {
                    operator = CompareOp.NOT_EQUAL;
                } else {
                    operator = CompareOp.EQUAL;
                }

                final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                        Bytes.toBytes(column), operator, new BinaryComparator(Bytes.toBytes(true)));
                filterColumn.setFilterIfMissing(!not);
                filter = filterColumn;
                recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                        operator.name(), Boolean.toString(true), false, !not);
            } else if (simpleCondition.getOperator().equals(Operator.IS_FALSE)) {
                CompareOp operator;
                if (not) {
                    operator = CompareOp.NOT_EQUAL;
                } else {
                    operator = CompareOp.EQUAL;
                }
                final SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                        Bytes.toBytes(column), operator, new BinaryComparator(Bytes.toBytes(false)));
                filterColumn.setFilterIfMissing(!not);
                filter = filterColumn;
                recordShellScanWithFiltersCommand(tableName, mappingMap, familyColumn, column, ParameterNaming.COLUMN_FILTER,
                        operator.name(), Boolean.toString(false), false, !not);
            }

            return filter;
        }
    }

    private static void checkConditionSyntax(final CustomWrapperSimpleCondition simpleCondition, final String familyColumn, boolean notCondition)
            throws CustomWrapperException {
        
        if ((familyColumn.equals(ParameterNaming.COL_STARTROW) || familyColumn.equals(ParameterNaming.COL_STOPROW))
                && ((!simpleCondition.getOperator().equals(Operator.EQUALS_TO) || notCondition) 
                        && (!simpleCondition.getOperator().equals(Operator.NOT_EQUALS_TO) || !notCondition))) {
            throw new CustomWrapperException(familyColumn + " only supports equals conditions");
        }
    }

    private static Object[] processRow(final Result resultSet, final Map<String, List<HBaseColumnDetails>> mappingMap, List<CustomWrapperFieldExpression> projectedFields) {

        final Object[] row = new Object[projectedFields.size()];

        int i = 0;
        for (CustomWrapperFieldExpression projectedField : projectedFields) {

            String projectedFieldName = projectedField.getName();
            if (projectedFieldName.equals(ParameterNaming.COL_ROWKEY)) {
                row[i] = Bytes.toString(resultSet.getRow());
            } else {
                List<HBaseColumnDetails> mappingColumns = mappingMap.get(projectedFieldName);
                
                if (mappingColumns != null) {
                    final NavigableMap<byte[], byte[]> resultFamily = resultSet.getFamilyMap(projectedFieldName.getBytes());
                    final Set<byte[]> resultColumns = resultFamily.keySet();

                    final Object[] subrow = new Object[mappingColumns.size()];

                    int j = 0;
                    for (final HBaseColumnDetails mappingColumn : mappingColumns) {
                        if (resultColumns.contains(mappingColumn.getName().getBytes())) {
                            byte[] content = resultFamily.get(mappingColumn.getName().getBytes());

                            if (mappingColumn.getType().equals(ParameterNaming.TYPE_TEXT)) {
                                subrow[j] = Bytes.toString(content);
                            } else if (mappingColumn.getType().equals(ParameterNaming.TYPE_INTEGER)) {

                                final int max_int = Integer.SIZE / Byte.SIZE;
                                if (content.length < max_int) {
                                    content = HbaseUtil.fillWithZeroBytes(content, max_int - content.length);
                                }

                                subrow[j] = Integer.valueOf(Bytes.toInt(content));
                            } else if (mappingColumn.getType().equals(ParameterNaming.TYPE_LONG)) {
                                final int max_long = Long.SIZE / Byte.SIZE;
                                if (content.length < max_long) {
                                    content = HbaseUtil.fillWithZeroBytes(content, max_long - content.length);
                                }

                                subrow[j] = Long.valueOf(Bytes.toLong(content));

                            } else if (mappingColumn.getType().equals(ParameterNaming.TYPE_FLOAT)) {
                                final int max_float = Float.SIZE / Byte.SIZE;
                                if (content.length < max_float) {
                                    content = HbaseUtil.fillWithZeroBytes(content, max_float - content.length);
                                }
                                subrow[j] = Float.valueOf(Bytes.toFloat(content));

                            } else if (mappingColumn.getType().equals(ParameterNaming.TYPE_DOUBLE)) {
                                final int max_long = Long.SIZE / Byte.SIZE;
                                if (content.length < max_long) {
                                    content = HbaseUtil.fillWithZeroBytes(content, max_long - content.length);
                                }
                                subrow[j] = Double.valueOf(Bytes.toDouble(content));
                            } else if (mappingColumn.getType().equals(ParameterNaming.TYPE_BOOLEAN)) {
                                subrow[j] = Boolean.valueOf(Bytes.toBoolean(content));
                            } else {
                                subrow[j] = content;
                            }
                        } else {
                            subrow[j] = null;
                        }
                        j++;
                    }
                    row[i] = subrow;

                }
            }
            
            i++;
        }

        return row;
    }

    private void recordShellGetCommand(final String tableName, final Map<String, List<HBaseColumnDetails>> mappingMap, final String value) {
        
        final StringBuilder commandSb = new StringBuilder();
        commandSb.append("get '").append(tableName).append("','").append(value).append("',{COLUMNS => [");

        for (final Map.Entry<String, List<HBaseColumnDetails>> entry : mappingMap.entrySet()) {
            String mappingFamily = entry.getKey();
            List<HBaseColumnDetails> mappingColumns = entry.getValue();
            for (final HBaseColumnDetails mappingColumn : mappingColumns) {
                commandSb.append("'").append(mappingFamily).append(':').append(mappingColumn.getName()).append("',");
            }
        }
        
        if (commandSb.charAt(commandSb.length() -1 ) == ',') {
            commandSb.deleteCharAt(commandSb.length() - 1);
        }
        commandSb.append("]}");

        String command = commandSb.toString();
        log(LOG_TRACE, "HBase shell command:" + command);
        getCustomWrapperPlan().addPlanEntry("HBase shell command " + this.filterNumber++, command);
    }

    private void recordShellScanCommand(final String tableName, final Map<String, List<HBaseColumnDetails>> mappingMap, String startRow,
            String stopRow) {
        
        final StringBuilder commandSb = new StringBuilder();
        commandSb.append("scan  '").append(tableName).append("',{COLUMNS => [");

        for (final Map.Entry<String, List<HBaseColumnDetails>> entry : mappingMap.entrySet()) {
            String mappingFamily = entry.getKey();
            List<HBaseColumnDetails> mappingColumns = entry.getValue();
            for (final HBaseColumnDetails mappingColumn : mappingColumns) {
                commandSb.append("'").append(mappingFamily).append(':').append(mappingColumn.getName()).append("',");
            }
        }
        
        if (commandSb.charAt(commandSb.length() -1 ) == ',') {
            commandSb.deleteCharAt(commandSb.length() - 1);
        }
        commandSb.append("]");
        
        if (startRow != null) {
            commandSb.append(",STARTROW => '").append(startRow).append("' ");
        }
        
        if (stopRow != null) {
            commandSb.append(",STOPROW => '").append(stopRow).append("' ");
        }
        commandSb.append("}\n");

        String command = commandSb.toString();
        
        log(LOG_TRACE, "HBase shell command:" + command);
        getCustomWrapperPlan().addPlanEntry("HBase shell command " + this.filterNumber++, command);
    }
    
    private void recordShellScanWithFiltersCommand(final String tableName, final Map<String, List<HBaseColumnDetails>> mappingMap,
            final String familyColumn, final String column, final String filter, final String operator, final String value,
            final boolean isRegex, final boolean filterIfMissing) {

        final StringBuilder commandSb = new StringBuilder();
        commandSb.append("Before executing this command in the shell you should import the following classes:\n");
        commandSb.append("import org.apache.hadoop.hbase.filter.CompareFilter \n");
        if (ParameterNaming.COLUMN_FILTER.equals(filter)) {
            commandSb.append("import org.apache.hadoop.hbase.filter.SingleColumnValueFilter \n");
        } else {
            commandSb.append("import org.apache.hadoop.hbase.filter.RowFilter \n");
        }

        if (isRegex) {
            commandSb.append("import org.apache.hadoop.hbase.filter.RegexStringComparator \n");
        } else {
            commandSb.append("import org.apache.hadoop.hbase.filter.BinaryComparator \n");
        }
        
        commandSb.append("import org.apache.hadoop.hbase.util.Bytes \n");
        commandSb.append("filter = ").append(filter).append(".new(");
        if (ParameterNaming.COLUMN_FILTER.equals(filter)) {
            commandSb.append("Bytes.toBytes('").append(familyColumn).append("'),");
            commandSb.append("Bytes.toBytes('").append(column).append("'),");
        }
        commandSb.append("CompareFilter::CompareOp.valueOf('").append(operator).append("'), ");
        if (isRegex) {
            commandSb.append("RegexStringComparator.new('").append(value).append("'");
        } else {
            commandSb.append("BinaryComparator.new(Bytes.toBytes('").append(value).append("')");
        }
        commandSb.append(")) \n");
        if (filterIfMissing) {
            commandSb.append("filter.setFilterIfMissing(true) \n");
        }

        commandSb.append("scan ");
        
        commandSb.append("'").append(tableName).append("',{ COLUMNS => [");
        for (final Map.Entry<String, List<HBaseColumnDetails>> entry : mappingMap.entrySet()) {
            String mappingFamily = entry.getKey();
            List<HBaseColumnDetails> mappingColumns = entry.getValue();
            for (final HBaseColumnDetails mappingColumn : mappingColumns) {
                commandSb.append("'").append(mappingFamily).append(':').append(mappingColumn.getName()).append("',");
            }
        }

        if (commandSb.charAt(commandSb.length() -1 ) == ',') {
            commandSb.deleteCharAt(commandSb.length() - 1);
        }
        commandSb.append("] ");
        commandSb.append(",FILTER => filter ");
        commandSb.append("}\n");

        String command = commandSb.toString();
        
        log(LOG_TRACE, "HBase shell command:" + command);
        getCustomWrapperPlan().addPlanEntry("HBase shell command " + this.filterNumber++, command);

    }

    @Override
    public boolean stop() {
        this.stopRequested = true;
        return this.stopRequested;
    }

    private static byte[] getBytesFromExpresion(final CustomWrapperSimpleExpression expression) {
        byte[] value;

        if (expression.getValue() instanceof Integer) {
            value = Bytes.toBytes(((Integer) expression.getValue()).intValue());
        } else if (expression.getValue() instanceof Long) {
            value = Bytes.toBytes(((Long) expression.getValue()).longValue());
        } else if (expression.getValue() instanceof Double) {
            value = Bytes.toBytes(((Double) expression.getValue()).doubleValue());
        } else if (expression.getValue() instanceof Float) {
            value = Bytes.toBytes(((Float) expression.getValue()).floatValue());
        } else if (expression.getValue() instanceof String) {
            value = Bytes.toBytes((String) expression.getValue());
        } else if (expression.getValue() instanceof Boolean) {
            value = Bytes.toBytes(((Boolean) expression.getValue()).booleanValue());
        } else {
            value = Bytes.toBytes((String) expression.getValue());
        }

        return value;
    }

}
