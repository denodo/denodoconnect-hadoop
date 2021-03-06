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
package com.denodo.connect.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.denodo.connect.hadoop.hbase.HBaseColumnDetails;
import com.denodo.connect.hadoop.hbase.commons.naming.ParameterNaming;

public final class HbaseUtil {

    private HbaseUtil() {

    }

    public static int getSQLType(final String mappingType) {
        if (mappingType.equals(ParameterNaming.TYPE_TEXT)) {
            return java.sql.Types.VARCHAR;
        } else if (mappingType.equals(ParameterNaming.TYPE_INTEGER)) {
            return java.sql.Types.INTEGER;
        } else if (mappingType.equals(ParameterNaming.TYPE_LONG)) {
            return java.sql.Types.BIGINT;
        } else if (mappingType.equals(ParameterNaming.TYPE_FLOAT)) {
            return java.sql.Types.FLOAT;
        } else if (mappingType.equals(ParameterNaming.TYPE_DOUBLE)) {
            return java.sql.Types.DOUBLE;
        } else if (mappingType.equals(ParameterNaming.TYPE_BOOLEAN)) {
            return java.sql.Types.BOOLEAN;
        } else {
            // other types will output the raw bytes in a string field
            return java.sql.Types.VARCHAR;
        }
    }

    public static Map<String, List<HBaseColumnDetails>> parseMapping(final String jsonMap) throws JSONException {
        
        final HashMap<String, List<HBaseColumnDetails>> structure = new LinkedHashMap<String, List<HBaseColumnDetails>>();
        final String mapping = jsonMap.replaceAll("\\\\", "");
        final JSONObject json = new JSONObject(mapping);
        if (isRowKeyColumn(json)) {
            return Collections.emptyMap();
        }
        for (int i = 0; i < json.length(); i++) {
            final String rowName = json.names().get(i).toString();
            try {
                final JSONObject subrow = json.getJSONObject(rowName);
                final List<HBaseColumnDetails> subrows = new ArrayList<HBaseColumnDetails>();
                for (int j = 0; j < subrow.length(); j++) {
                    final String subrowName = subrow.names().get(j).toString();
                    subrows.add(new HBaseColumnDetails(subrowName, subrow.getString(subrowName)));
                }
                structure.put(rowName, subrows);
            } catch (JSONException e) {
                throw new IllegalArgumentException("'" + rowName + "' is not defined as a column family. Only '" + ParameterNaming.COL_ROWKEY
                        + "' could be provided as is, without a column family", e);
            }
        }

        return structure;
    }

    /**
     * Checks if json contains only row_key field. Retrieving only the row_key
     * field of data set is a special case: for performance reasons (Redmine #17877)
     */
    private static boolean isRowKeyColumn(JSONObject json) {
        return (json.length() == 1) && json.has(ParameterNaming.COL_ROWKEY);        
    }

    public static String getRegExpformLike(final String expr)
    {
        String regex = quotemeta(expr);

        regex = regex.replace("_", ".").replace("%", ".*?");
        regex = "^" + regex + "$";
        return regex;
    }

    public static String quotemeta(final String s)
    {
        if (s == null)
        {
            throw new IllegalArgumentException("String cannot be null");
        }

        final int len = s.length();
        if (len == 0)
        {
            return "";
        }

        final StringBuilder sb = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++)
        {
            final char c = s.charAt(i);
            if ("[](){}.*+?$^|#\\".indexOf(c) != -1)
            {
                sb.append("\\");
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Fill a byte[] with 0 in the start.
     * 
     * @param item
     * @param numberOfSpaces
     * @return byte[]
     */
    public static byte[] fillWithZeroBytes(final byte[] item, final int numberOfSpaces) {
        final byte[] result = new byte[item.length + numberOfSpaces];
        Arrays.fill(result, 0, numberOfSpaces, (byte) 0);
        System.arraycopy(item, 0, result, numberOfSpaces, item.length);
        return result;
    }
}
