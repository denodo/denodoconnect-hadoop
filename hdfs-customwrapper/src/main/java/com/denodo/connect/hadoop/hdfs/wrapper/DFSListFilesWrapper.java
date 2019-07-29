/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2018, Denodo Technologies (http://www.denodo.com)
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
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;
import com.denodo.connect.hadoop.hdfs.wrapper.util.filesystem.FileSystemUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterRouteValue;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterValue;

public class DFSListFilesWrapper extends AbstractSecureHadoopWrapper {


    private static final  Logger LOG = LoggerFactory.getLogger(DFSListFilesWrapper.class);
    

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
            new CustomWrapperInputParameter[] {
                    new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                        "e.g. hdfs://<ip>:<port> or s3n://<id>:<secret>\\\\@<bucket>t ",
                        true, CustomWrapperInputParameterTypeFactory.stringType()),
                    new CustomWrapperInputParameter(Parameter.HDFS_SITE_PATH,
                            "Local route of hdfs-site.xml configuration file ",
                            false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP}))
            };
    
    
    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(INPUT_PARAMETERS, super.getInputParameters());
    }    
    
    
    @Override
    public CustomWrapperConfiguration getConfiguration() {

        final CustomWrapperConfiguration configuration = new CustomWrapperConfiguration();
        configuration.setDelegateProjections(false); // doing the selection here or in VDP does not change performance in a significant way
                                                     // and same for conditions delegation
        
        return configuration;
    }
    
    
    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(final Map<String, String> inputValues)
            throws CustomWrapperException {
        
        final boolean searchable = true;
        final boolean updateable = true;
        final boolean nullable = true;
        final boolean mandatory = true;
        
        return new CustomWrapperSchemaParameter[] {
                new CustomWrapperSchemaParameter(Parameter.PARENT_FOLDER, java.sql.Types.VARCHAR, null, !searchable,
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, !updateable, !nullable, mandatory),
                new CustomWrapperSchemaParameter(Parameter.RELATIVE_PATH, java.sql.Types.VARCHAR, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.FILE_NAME, java.sql.Types.VARCHAR, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.EXTENSION, java.sql.Types.VARCHAR, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.FULL_PATH, java.sql.Types.VARCHAR, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.PATH_WITHOUT_SCHEME, java.sql.Types.VARCHAR, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.FILE_TYPE, java.sql.Types.VARCHAR, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.ENCRYPTED, java.sql.Types.BOOLEAN, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.DATE_MODIFIED, java.sql.Types.TIMESTAMP, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.OWNER, java.sql.Types.VARCHAR, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.GROUP, java.sql.Types.VARCHAR, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.PERMISSIONS, java.sql.Types.VARCHAR, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.SIZE, java.sql.Types.INTEGER, null, !searchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !updateable, nullable, !mandatory),
                new CustomWrapperSchemaParameter(Parameter.RECURSIVE, java.sql.Types.BOOLEAN, null, !searchable,
                        CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, !updateable, nullable, mandatory)
        };
    }


    @Override
    public void doRun(final CustomWrapperConditionHolder condition, final List<CustomWrapperFieldExpression> projectedFields,
            final CustomWrapperResult result, final Map<String, String> inputValues) throws CustomWrapperException {

        final Configuration conf = getHadoopConfiguration(inputValues);

        final Map<CustomWrapperFieldExpression, Object> conditions = condition.getConditionMap(true);
        final String parentFolder = (String) getMandatoryField(Parameter.PARENT_FOLDER, conditions);
        final boolean recursive = ((Boolean) getMandatoryField(Parameter.RECURSIVE, conditions)).booleanValue();


        FileSystem fileSystem = null;
        try {
            
            fileSystem = FileSystem.get(conf);

            final RemoteIterator<LocatedFileStatus> fileIterator = FileSystemUtils.listFiles(fileSystem, new Path(parentFolder),
                    recursive);
            while (fileIterator.hasNext() && !isStopRequested()) {

                final FileStatus fileStatus = fileIterator.next();
                
                final Object[] row = new Object[projectedFields.size()];
                
                int index = 0;
                row[index++] = parentFolder;
                
                final Path path = fileStatus.getPath();
                final Path pathWithoutScheme = Path.getPathWithoutSchemeAndAuthority(path);
                final String relativePath = getRelativePath(parentFolder, path, pathWithoutScheme);
                row[index++] = relativePath;
                
                final String fileName = path.getName();
                row[index++] = fileName;
                
                row[index++] = fileStatus.isFile() ? FilenameUtils.getExtension(fileName) : null;
                row[index++] = path.toString();
                row[index++] = pathWithoutScheme.toString();
                row[index++] = getType(fileStatus);
                row[index++] = Boolean.valueOf(fileStatus.isEncrypted());
                row[index++] = new Date(fileStatus.getModificationTime());
                
                row[index++] = fileStatus.getOwner();
                row[index++] = fileStatus.getGroup();
                row[index++] = fileStatus.getPermission().toString();  
                
                row[index++] = fileStatus.isFile() ? Long.valueOf(fileStatus.getLen()) : null;
                row[index++] = Boolean.valueOf(recursive);

                result.addRow(row, projectedFields);
            }

        } catch (final Exception e) {
            LOG.error("Error listing files", e);
            throw new CustomWrapperException("Error listing files: " + e.getMessage(), e);
        } finally {
            try {
                if (fileSystem != null) {
                    fileSystem.close();
                }
            } catch (final IOException e) {
                LOG.error("Error closing file system", e);
            }

        }

    }

    private Object getMandatoryField(final String fieldName, final Map<CustomWrapperFieldExpression, Object> conditions) {

        if (conditions != null) {
            for (final Entry <CustomWrapperFieldExpression, Object> entry : conditions.entrySet()) {
                final CustomWrapperFieldExpression field = entry.getKey();
                final Object value = entry.getValue();
                if (field.getName().equals(fieldName)) {
                    return value;
                }
            }
        }

        return null;
    }
    
    private String getRelativePath(final String parentFolder, final Path fullPath, final Path fullPathWithoutScheme) {
        
        final Path parentFolderPath = new Path(parentFolder);
        final Path path = (parentFolderPath.isAbsoluteAndSchemeAuthorityNull()) ? fullPathWithoutScheme : fullPath;
        
        return StringUtils.substringAfter(path.getParent().toString(), parentFolder);        
    }    
    
    private String getType(final FileStatus fileStatus) {
        
        String type = Parameter.TYPE_FILE;
        if (fileStatus.isDirectory()) {
            type = Parameter.TYPE_DIR;
        } else if (fileStatus.isSymlink()) {
            type = Parameter.TYPE_SYMLINK;
        }
        
        return type;
    }
    
}