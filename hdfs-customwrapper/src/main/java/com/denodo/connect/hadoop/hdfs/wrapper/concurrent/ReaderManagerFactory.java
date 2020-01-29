/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2020, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.hdfs.wrapper.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ReaderManagerFactory {

    private static final Map<String, ReaderManager> readerManagers = new ConcurrentHashMap<>();

    public static ReaderManager get(final String fileSystemURI, final int threadPoolSize) {

        ReaderManager readerManager = readerManagers.get(fileSystemURI);

        // ConcurrentHashMap::computeIfAbsent is the best option but it is only available since java1.8 and
        // Denodo 6 should be compatible with java1.7
        synchronized (readerManagers) {

            if (! readerManagers.containsKey(fileSystemURI)) {
                readerManager = new ReaderManager(threadPoolSize);
                readerManagers.put(fileSystemURI, readerManager);
            }

            readerManager.resizePool(threadPoolSize);  // we don't know whether the user changed the threadPoolSize parameter value
        }


        return readerManager;

    }

}