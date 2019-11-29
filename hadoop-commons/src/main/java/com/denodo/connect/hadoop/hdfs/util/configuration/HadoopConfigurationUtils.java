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

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

public final class HadoopConfigurationUtils {


    private HadoopConfigurationUtils() {

    }

    /**
     * @param fileSystemURIString A URI whose scheme and authority determine the
     *        FileSystem implementation. The uri's scheme determines the config property
     *        (fs.SCHEME.impl) naming the FileSystem implementation class.
     *        The uri's authority is used to determine the host, port, etc. for a filesystem.
     *        E.g. HDFS -> hdfs://ip:port
     * @return the basic hadoop configuration
     */
    public static Configuration getConfiguration(final String fileSystemURIString, final InputStream... customFiles) {

        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", fileSystemURIString);

        disableFileSystemCache(fileSystemURIString, conf);

        allowGeneralKerberosPrincipals(conf);

        
        for (final InputStream customFile: customFiles) {
            if (customFile != null) {
                conf.addResource(customFile);
            }
        }

        return conf;
    }

    /*
     * FileSystem.get returns the same object for every invocation with the same filesystem.
     * So if one is closed anywhere, they are all closed. (#41229 - error when joining files residing in a hdfs filesystem)
     * This setting prevents a FileSystem object from being shared by multiple clients, because it would prevent,
     * for example, two callers of FileSystem#get() from closing each other's filesystem.
     *
     * This setting is required too as FileSystem#close() is necessary for #39931: Failed connections could require
     *  restarting VDP to refresh wrapper configuration files.
     */
    private static void disableFileSystemCache(final String fileSystemURIString, final Configuration conf) {

        final URI fileSystemURI = URI.create(fileSystemURIString);
        final String disableCacheName = String.format("fs.%s.impl.disable.cache", fileSystemURI.getScheme());
        conf.setBoolean(disableCacheName, true);
    }

    /*
     * General pattern that avoids having to specify the server's Kerberos principal name when using Kerberos authentication
     */
    private static void allowGeneralKerberosPrincipals(final Configuration conf) {
        conf.set("dfs.namenode.kerberos.principal.pattern", "*");
    }


}
