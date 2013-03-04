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
package com.denodo.connect.hadoop.hdfs.wrapper.util.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public final class HadoopConfigurationUtils {

    private static final Logger logger = Logger.getLogger(HadoopConfigurationUtils.class);

    private HadoopConfigurationUtils() {

    }

    /**
     * @return the basic hadoop configuration (only including datanode ip, port)
     */
    public static Configuration getConfiguration(String dataNodeIP, String dataNodePort) {

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + dataNodeIP + ":" + dataNodePort);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        logger.debug("Returning configuration: " + conf
            + " - value of 'fs.default.name' -> " + conf.get("fs.default.name")
            + " - value of 'fs.hdfs.impl' -> " + conf.get("fs.hdfs.impl"));
        return conf;
    }

}
