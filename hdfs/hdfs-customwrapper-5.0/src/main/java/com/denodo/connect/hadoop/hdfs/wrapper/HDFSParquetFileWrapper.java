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
package com.denodo.connect.hadoop.hdfs.wrapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSFileReader;
import com.denodo.connect.hadoop.hdfs.reader.HDFSParquetFileReader;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;
import com.denodo.connect.hadoop.hdfs.util.schema.VDPSchemaUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;

/**
 * HDFS file custom wrapper for reading Parquet files stored in HDFS (Hadoop
 * Distributed File System).
 * <p>
 *
 * The following parameters are required: file system URI, Avro
 * schema file path or Avro schema JSON. <br/>
 *
 */
public class HDFSParquetFileWrapper extends AbstractSecureHadoopWrapper {

    private static final Logger logger = Logger.getLogger(HDFSParquetFileWrapper.class);

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                "e.g. hdfs://<ip>:<port> or s3n://<id>:<secret>\\\\@<bucket>t ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.PARQUET_FILE_PATH,
                "Parquet File Path",
                true, CustomWrapperInputParameterTypeFactory.stringType()),          
            new CustomWrapperInputParameter(Parameter.CORE_SITE_PATH,
                "Local route of core-site.xml configuration file ",
                false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL})),
            new CustomWrapperInputParameter(Parameter.HDFS_SITE_PATH,
                "Local route of hdfs-site.xml configuration file ",
                false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL}))       
    };

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(INPUT_PARAMETERS, super.getInputParameters());
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        CustomWrapperConfiguration conf = super.getConfiguration();
        conf.setDelegateProjections(false);

        return conf;
    }

    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(Map<String, String> inputValues)
            throws CustomWrapperException {

        try {

            String fileSystemURI = inputValues.get(Parameter.FILESYSTEM_URI);
            String coreSitePath = inputValues.get(Parameter.CORE_SITE_PATH);
            String hdfsSitePath = inputValues.get(Parameter.HDFS_SITE_PATH);
            Configuration conf = HadoopConfigurationUtils.getConfiguration(fileSystemURI, coreSitePath, hdfsSitePath);

            String parquetFilePath = inputValues.get(Parameter.PARQUET_FILE_PATH);
            Path path = new Path(parquetFilePath);

            HDFSFileReader reader = null;
            reader= new HDFSParquetFileReader(conf, path, null, null);

            SchemaElement javaSchema =((HDFSParquetFileReader) reader).getSchema( null, path, conf);

            return  VDPSchemaUtils.buildSchemaParameter(javaSchema.getElements());

        } catch (Exception e) {
            logger.error("Error building wrapper schema", e);
            throw new CustomWrapperException(e.getMessage(), e);
        }

    }


    @Override
    public void doRun(CustomWrapperConditionHolder condition, List<CustomWrapperFieldExpression> projectedFields,
            CustomWrapperResult result, Map<String, String> inputValues) throws CustomWrapperException {

        String fileSystemURI = inputValues.get(Parameter.FILESYSTEM_URI);
        String parquetFilePath = inputValues.get(Parameter.PARQUET_FILE_PATH);
      
        String coreSitePath = inputValues.get(Parameter.CORE_SITE_PATH);
        String hdfsSitePath = inputValues.get(Parameter.HDFS_SITE_PATH);
        Configuration conf = HadoopConfigurationUtils.getConfiguration(fileSystemURI, coreSitePath, hdfsSitePath);


        Path path = new Path(parquetFilePath);

        HDFSFileReader reader = null;
        try {


            reader = new HDFSParquetFileReader(conf, path, null, projectedFields);
            Object[] rowData = new Object[1];

            Object parquetData = reader.read();
            while (parquetData != null && !isStopRequested()) {
                result.addRow( (Object[])parquetData, projectedFields);

                parquetData = reader.read();
            }

         

        } catch (Exception e) {
            logger.error("Error accessing Parquet file", e);
            throw new CustomWrapperException("Error accessing Parquet file: " + e.getMessage(), e);

        } finally {
            try {
                if (reader != null ) {
                    reader.close();
                }
            } catch (IOException e) {
                logger.error("Error releasing the reader", e);
            }

        }
    }



}
