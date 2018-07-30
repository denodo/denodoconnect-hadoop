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
package com.denodo.connect.hadoop.hdfs.util.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HadoopConfigurationUtils {

    private static final  Logger LOG = LoggerFactory.getLogger(HadoopConfigurationUtils.class);

    private HadoopConfigurationUtils() {

    }

    /**
     * @param fileSystemURI A URI whose scheme and authority determine the
     *        FileSystem implementation. The uri's scheme determines the config property
     *        (fs.SCHEME.impl) naming the FileSystem implementation class.
     *        The uri's authority is used to determine the host, port, etc. for a filesystem.
     *        E.g. HDFS -> hdfs://ip:port
     *             AMAZON S3 -> s3n://id:secret@bucket (Note that since the secret
     *             access key can contain slashes, you must remember to escape them
     *             by replacing each slash / with the string %2F.)
     * @return the basic hadoop configuration
     */
    public static Configuration getConfiguration(final String fileSystemURI, final String... customFilePathNames) {

        final Configuration conf = new Configuration();
        conf.set("fs.default.name", fileSystemURI);

        // General pattern that avoids having to specify the server's Kerberos principal name when using Kerberos authentication
        conf.set("dfs.namenode.kerberos.principal.pattern", "*");

        LOG.debug("Returning configuration: " + conf
            + " - value of 'fs.default.name' -> " + conf.get("fs.default.name"));
        
        for (final String customFilePathName: customFilePathNames) {
            if (customFilePathName != null) {
                conf.addResource(new Path(customFilePathName));
            }
        }
        
        
        return conf;
    }

}
