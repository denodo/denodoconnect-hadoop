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
package com.denodo.connect.hadoop.hbase.commons.naming;

public final class ParameterNaming {

    public static final String CONF_HBASE_IP = "hbaseIP";
    public static final String CONF_HBASE_PORT = "hbasePort";
    public static final String CONF_TABLE_NAME = "tableName";
    public static final String CONF_TABLE_MAPPING = "tableMapping";

    public static final String CONF_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String CONF_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

    public static final String TYPE_TEXT = "text";
    public static final String TYPE_INTEGER = "int";
    public static final String TYPE_LONG = "long";
    public static final String TYPE_FLOAT = "float";
    public static final String TYPE_DOUBLE = "double";
    public static final String TYPE_BOOLEAN = "boolean";

    public static final String COL_ROWKEY = "row_key";

    private ParameterNaming() {

    }

}
