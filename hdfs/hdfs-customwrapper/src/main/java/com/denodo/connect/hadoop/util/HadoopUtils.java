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
package com.denodo.connect.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.commons.exception.DeleteFileException;
import com.denodo.connect.hadoop.util.configuration.HadoopConfigurationUtils;

public class HadoopUtils {

    private static final Logger logger = Logger.getLogger(HadoopUtils.class);

    /**
     * It deletes the given file or folder
     * 
     */
    public static void deleteFile(String dataNodeIp, String dataNodePort, String hadoopKeyClass, String hadoopValueClass, Path outputPath) {
        try {
            logger.debug("Deleting... '" + outputPath + "'"); //$NON-NLS-1$ //$NON-NLS-2$
            Configuration configuration = HadoopConfigurationUtils.getConfiguration(dataNodeIp, dataNodePort, hadoopKeyClass,
                    hadoopValueClass);
            FileSystem.get(configuration).delete(outputPath, true);
        } catch (IOException e) {
            throw new DeleteFileException(outputPath);
        }
    }
}
