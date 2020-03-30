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
    public static final String QUOTE = "Quote";
    public static final String COMMENT_MARKER = "Comment marker";
    public static final String ESCAPE = "Escape";
    public static final String IGNORE_SPACES = "Ignore spaces";
    public static final String HEADER = "Header";
    public static final String NULL_VALUE = "Null value";
    public static final String IGNORE_MATCHING_ERRORS = "Ignore matching errors";
    public static final String FILE_PATH = "Path";
    public static final String FILE_NAME_PATTERN = "File name pattern";
    public static final String FILE_ENCODING = "File encoding";

    public static final String DELETE_AFTER_READING = "Delete after reading";

    public static final String INCLUDE_PATH_COLUMN = "Include full path column";
    
    public static final String CORE_SITE_PATH = "Custom core-site.xml file";
    public static final String HDFS_SITE_PATH = "Custom hdfs-site.xml file";

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

    public static final String PARQUET_FILE_PATH = "Parquet File path";
    public static final String PARALLELISM_TYPE = "Parallelism type";
    public static final String FILE_PARALLEL = "Parallelism by File";
    public static final String COLUMN_PARALLEL = "Parallelism by Column";
    public static final String NOT_PARALLEL = "No Parallelism";
    public static final String AUTOMATIC_PARALLELISM = "Automatic";
    public static final String PARALLELISM_LEVEL = "Parallelism level";
    public static final String CLUSTER_PARTITION_FIELDS = "Cluster/partition fields";
    public static final String THREADPOOL_SIZE = "Thread Pool size";

    public static final String ACCESS_KEY_ID = "Access Key ID";
    public static final String SECRET_ACCESS_KEY = "Secret Access Key";
    public static final String IAM_ROLE_ASSUME = "IAM Role to Assume";
    public static final String ENDPOINT = "Endpoint";

    public static final String USE_EC2_IAM_CREDENTIALS = "Use EC2 IAM credentials";


    public static final String RECURSIVE = "recursive";
    
    public static final String PARENT_FOLDER = "parentfolder";
    public static final String RELATIVE_PATH = "relativepath";
    public static final String FILE_NAME = "filename";
    public static final String EXTENSION = "extension";
    public static final String FULL_PATH = "fullpath";
    public static final String PATH_WITHOUT_SCHEME = "pathwithoutscheme";
    public static final String FILE_TYPE = "filetype";
    public static final String ENCRYPTED = "encrypted";
    public static final String DATE_MODIFIED = "datemodified";
    public static final String OWNER = "owner";
    public static final String GROUP = "group";
    public static final String PERMISSIONS = "permissions";
    public static final String SIZE = "size";
    
    public static final String TYPE_FILE = "file";
    public static final String TYPE_DIR = "directory";
    public static final String TYPE_SYMLINK = "symbolic link";

    public static final String AWS_CREDENTIALS_PROVIDER = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";
    public static final String AWS_ASSUMED_ROLE_PROVIDER = "com.denodo.connect.hadoop.hdfs.wrapper.providers.AWSAssumedRoleCredentialProvider";
    public static final String INSTANCE_PROFILE_CREDENTIALS_PROVIDER = "com.amazonaws.auth.InstanceProfileCredentialsProvider";

    private Parameter() {

    }

}
