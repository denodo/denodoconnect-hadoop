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
package com.denodo.connect.dfs.wrapper;


import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.denodo.connect.dfs.commons.naming.Parameter;
import com.denodo.connect.dfs.reader.DFSFileReader;
import com.denodo.connect.dfs.reader.keyvalue.DFSMapFileReader;
import com.denodo.connect.dfs.util.type.TypeUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * DFS file custom Wrapper for reading map files stored in a DFS (Distributed File System: hdfs, S3, ...).
 * <p>
 *
 * The following parameters are required: file system URI, file path,
 * Hadoop key class name and Hadoop value class name. <br/>
 *
 * Key/value pairs contained in the file will be returned by the wrapper.
 * </p>
 *
 */
public class DFSMapFileWrapper extends AbstractDFSKeyValueFileWrapper {

    private static final  CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.HADOOP_KEY_CLASS,
                "Hadoop key class", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.HADOOP_VALUE_CLASS,
                "Hadoop value class", true,
                 CustomWrapperInputParameterTypeFactory.stringType())
        };

    public DFSMapFileWrapper() {
        super();
    }

    @Override
    public CustomWrapperInputParameter[] doGetInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(super.doGetInputParameters(), INPUT_PARAMETERS);
    }

    @Override
    public DFSFileReader getDFSFileReader(final Map<String, String> inputValues, final boolean getSchemaParameters)
        throws IOException, InterruptedException, CustomWrapperException {

        final Configuration conf = getConfiguration(inputValues);

        final String hadoopKeyClass = TypeUtils.getHadoopClass(inputValues.get(Parameter.HADOOP_KEY_CLASS));
        final String hadoopValueClass = TypeUtils.getHadoopClass(inputValues.get(Parameter.HADOOP_VALUE_CLASS));

        final String inputFilePath = StringUtils.trim(inputValues.get(Parameter.FILE_PATH));
        final Path path = new Path(inputFilePath);
        
        final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);

        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

        return new DFSMapFileReader(conf, hadoopKeyClass, hadoopValueClass, path, fileNamePattern, null, includePathColumn&&!getSchemaParameters);
    }
}
