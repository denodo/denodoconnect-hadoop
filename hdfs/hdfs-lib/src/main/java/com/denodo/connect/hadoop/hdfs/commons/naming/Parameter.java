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
package com.denodo.connect.hadoop.hdfs.commons.naming;

public final class Parameter {

    /* SSH Credentials */
    public static final String PASSWORD = "Password";
    public static final String PASSPHRASE = "Passphrase";
    public static final String KEY_FILE = "Key file";

    /* Kerberos Credentials */
    public static final String KERBEROS_ENABLED = "Kerberos enabled";
    public static final String PRINCIPAL = "Kerberos principal name";
    public static final String KERBEROS_PASWORD = "Kerberos password";
    public static final String KEYTAB = "Kerberos keytab file";
    public static final String KDC = "Kerberos Distribution Center";


    public static final String PATH_TO_JAR_IN_HOST = "Path to jar in host";

    public static final String MAPREDUCE_PARAMETERS = "MapReduce job parameters ";

    public static final String OUTPUTFILE_HANDLER = "Output file type ";

    public static final String FILESYSTEM_URI = "File system URI";
    public static final String HOST_IP = "Host IP";
    public static final String HOST_PORT = "Host port";
    public static final String USER = "User";

    public static final String HADOOP_KEY_CLASS = "Key class";
    public static final String HADOOP_VALUE_CLASS = "Value class";

    public static final String SEPARATOR = "Separator";
    public static final String FILE_PATH = "Path";

    public static final String DELETE_AFTER_READING = "Delete after reading";

    /**
     * The path to the .avsc file containing the Avro schema.
     * The two input parameters AVSC_FILE_PATH and AVSC_JSON are mutually exclusive.
     */
    public static final String AVRO_SCHEMA_PATH = "Avro schema path";

    /**
     * The Avro Schema as JSON text.
     * The two input parameters AVSC_FILE_PATH and AVSC_JSON are mutually exclusive.
     */
    public static final String AVRO_SCHEMA_JSON = "Avro schema JSON";

    public static final String KEY = "KEY";
    public static final String VALUE = "VALUE";

    public static final String AVRO_FILE_PATH = "avroFilepath";


    private Parameter() {

    }

}
