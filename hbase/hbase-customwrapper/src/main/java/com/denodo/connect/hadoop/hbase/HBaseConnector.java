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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class HBaseConnector extends AbstractCustomWrapper {

	private static final String CONF_TABLE_NAME = "tableName";
	private static final String CONF_TABLE_MAPPING = "tableMapping";

	private static final String TYPE_TEXT = "text";
	private static final String TYPE_INTEGER = "int";
	private static final String TYPE_LONG = "long";
	private static final String TYPE_FLOAT = "float";
	private static final String TYPE_DOUBLE = "double";

	private static final String COL_ROWKEY = "row_key";

	private static final Logger logger = Logger.getLogger(HBaseConnector.class);

	@Override
	public CustomWrapperInputParameter[] getInputParameters() { 
		return new CustomWrapperInputParameter[] {
				new CustomWrapperInputParameter(CONF_TABLE_NAME, true),
				new CustomWrapperInputParameter(CONF_TABLE_MAPPING, true)
		};
	}

	private boolean stopRequested = false;

	@Override
	public CustomWrapperConfiguration getConfiguration() {

		CustomWrapperConfiguration configuration = new CustomWrapperConfiguration();
		configuration.setDelegateProjections(true);
		configuration.setDelegateNotConditions(false);
		configuration.setDelegateOrConditions(false);
		configuration.setAllowedOperators(new String[] { CustomWrapperCondition.OPERATOR_EQ });

		return configuration;
	}


	public CustomWrapperSchemaParameter[] getSchemaParameters(
			Map<String, String> inputValues) throws CustomWrapperException {

		String mapping = inputValues.get(CONF_TABLE_MAPPING);
		try {
			Map<String,List<HBaseColumnDetails>> mappingMap = parseMapping(mapping);

			ArrayList<CustomWrapperSchemaParameter> rows = new ArrayList<CustomWrapperSchemaParameter>();

			//row ID
			rows.add(new CustomWrapperSchemaParameter(COL_ROWKEY,java.sql.Types.VARCHAR, null,
					true, CustomWrapperSchemaParameter.NOT_SORTABLE,false,true,false));

			// output schema based on the provided json
			for (String col:mappingMap.keySet()) {
				ArrayList<CustomWrapperSchemaParameter> subrows = new ArrayList<CustomWrapperSchemaParameter>();
				for (HBaseColumnDetails subrowData:mappingMap.get(col)) {
					CustomWrapperSchemaParameter subrow = null;

					subrow = new CustomWrapperSchemaParameter(subrowData.getName(), getSQLType(subrowData.getType()), null,
							false, CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false);
					subrows.add(subrow);

				}
				CustomWrapperSchemaParameter row = new CustomWrapperSchemaParameter(col, java.sql.Types.STRUCT, subrows.toArray(new CustomWrapperSchemaParameter[]{}));
				rows.add(row);
			}

			return rows.toArray(new CustomWrapperSchemaParameter[]{});

		} catch (Exception e) {
			e.printStackTrace();
			throw new CustomWrapperException("Error in mapping format: "+e.getMessage());
		}

	}


	public void run(CustomWrapperConditionHolder condition,
			List<CustomWrapperFieldExpression> projectedFields,
			CustomWrapperResult result, Map<String, String> inputValues)
	throws CustomWrapperException {

		Map<String,List<HBaseColumnDetails>> mappingMap;
		try {
			String mapping = inputValues.get(CONF_TABLE_MAPPING);
			mappingMap = parseMapping(mapping);
		} catch (Exception e) {
			throw new CustomWrapperException("Error in mapping format: "+e);
		}

		// Connects to HBase server
		String tableName = inputValues.get(CONF_TABLE_NAME);
		Configuration config = HBaseConfiguration.create();
		HTable table;
		try {
			//Get table metadata 
			table = new HTable(config, tableName);
			Set<byte[]> families = table.getTableDescriptor().getFamiliesKeys();

			Map<CustomWrapperFieldExpression, Object> conditionMap = condition.getConditionMap();

			// Get one single row
			if (conditionMap != null && !conditionMap.isEmpty() && conditionMap.size() == 1) {
				for (CustomWrapperFieldExpression field : conditionMap.keySet()) {
					String value = (conditionMap.get(field)).toString();
					Get get = new Get(value.getBytes());
					for (String family:mappingMap.keySet()) {
						for (HBaseColumnDetails subrowData:mappingMap.get(family)) {
							get.addColumn(family.getBytes(), subrowData.getName().getBytes());
						}
					}
					Result resultRow = table.get(get);
					if (resultRow != null && resultRow.getRow() != null) {
						Object[] rowArray = processRow(resultRow, mappingMap, families);
						result.addRow(rowArray, getGenericOutputpStructure(mappingMap));
					}
				}

				// Scan the whole table
			} else {
				Scan s = new Scan();
				// For performance reasons, just add to the scanner the families and qualifiers
				// specified in the mapping
				for (String family:mappingMap.keySet()) {
					for (HBaseColumnDetails subrowData:mappingMap.get(family)) {
						s.addColumn(family.getBytes(), subrowData.getName().getBytes());
					}
				}
				
				/*
				 * COMPLEX FILTER DELEGATION, CACHING, AND OTHER COMPLEX HBASE SCANNING 
				 * FEATURES COULD BE ADDED HERE
				 */
				ResultScanner scanner = table.getScanner(s);
				try {
					for (Result resultRow : scanner) {
						// Stop the scan if requested from outside
						if (this.stopRequested) {
							break;
						}
						// add the row to the output
						Object[] rowArray = processRow(resultRow, mappingMap, families);
						result.addRow(rowArray, getGenericOutputpStructure(mappingMap));
					}

				} finally {
					scanner.close();
				}
			}

		} catch (Exception e) {
			throw new CustomWrapperException("Error accessing the HBase table: "+e);
		}

	}

	private static Object[] processRow(Result resultSet, Map<String,List<HBaseColumnDetails>> mappingMap,
			Set<byte[]> families) {
		if (logger.isDebugEnabled()) {
			logger.debug("HBASE row: " + resultSet.toString());
		}

		// Iterates through the families if they are mapped
		Object[] rowArray = new Object[mappingMap.keySet().size()+1];

		int i = 0;
		for (String mappingFaimilyName : mappingMap.keySet()) {     

			// the row contains the mapped family
			if (families.contains(mappingFaimilyName.getBytes())) {
				NavigableMap<byte[], byte[]> familyMap = resultSet.getFamilyMap(mappingFaimilyName.getBytes());

				Set<byte[]> keys = familyMap.keySet();
				Object[] subrowArray = new Object[mappingMap.get(mappingFaimilyName).size()];
				int j = 0;
				// And fills the sub-rows
				for (HBaseColumnDetails subrowData:mappingMap.get(mappingFaimilyName)) {
					if (keys.contains(subrowData.getName().getBytes())) {
						if (subrowData.getType().equals(TYPE_TEXT)) {
							subrowArray[j] = Bytes.toString(familyMap.get(subrowData.getName().getBytes()));
						} else if (subrowData.getType().equals(TYPE_INTEGER)) {
							subrowArray[j] = Bytes.toInt(familyMap.get(subrowData.getName().getBytes()));
						} else if (subrowData.getType().equals(TYPE_LONG)) {
							subrowArray[j] = Bytes.toLong(familyMap.get(subrowData.getName().getBytes()));
						} else if (subrowData.getType().equals(TYPE_FLOAT)) {
							subrowArray[j] = Bytes.toFloat(familyMap.get(subrowData.getName().getBytes()));
						} else if (subrowData.getType().equals(TYPE_DOUBLE)) {
							subrowArray[j] = Bytes.toDouble(familyMap.get(subrowData.getName().getBytes()));
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

	private static Map<String,List<HBaseColumnDetails>> parseMapping(String jsonMap) throws JSONException {
		HashMap<String,List<HBaseColumnDetails>> structure = new HashMap<String,List<HBaseColumnDetails>>();

		String mapping = jsonMap.replaceAll("\\\\", "");

		JSONObject json = new JSONObject(mapping);
		for (int i = 0; i< json.length(); i++) {
			String rowName = json.names().get(i).toString();
			JSONObject subrow = json.getJSONObject(rowName);
			List<HBaseColumnDetails> subrows = new ArrayList<HBaseColumnDetails>();
			for (int j = 0; j< subrow.length(); j++) {
				String subrowName = subrow.names().get(j).toString();
				subrows.add(new HBaseColumnDetails(subrowName, subrow.getString(subrowName)));
			}
			structure.put(rowName, subrows);
		}

		return structure;
	}

	private List<CustomWrapperFieldExpression> getGenericOutputpStructure(Map<String,List<HBaseColumnDetails>> mapping) {
		List<CustomWrapperFieldExpression> output = new ArrayList<CustomWrapperFieldExpression>();

		for (String family: mapping.keySet()) {
			output.add(new CustomWrapperFieldExpression(family));
		}
		output.add(new CustomWrapperFieldExpression(COL_ROWKEY));
		return output;
	}

	private int getSQLType(String mappingType) {
		if (mappingType.equals(TYPE_TEXT)) {
			return java.sql.Types.VARCHAR;
		} else if (mappingType.equals(TYPE_INTEGER)) {
			return java.sql.Types.INTEGER;
		} else if (mappingType.equals(TYPE_LONG)) {
			return java.sql.Types.BIGINT;
		} else if (mappingType.equals(TYPE_FLOAT)) {
			return java.sql.Types.FLOAT;
		} else if (mappingType.equals(TYPE_DOUBLE)) {
			return java.sql.Types.DOUBLE;
		} else {
			// other types will output the raw bytes in a string field 
			return java.sql.Types.VARCHAR;
		}
	}

	@Override
	public boolean stop() {
		this.stopRequested = true;
		return this.stopRequested;
	}

}
