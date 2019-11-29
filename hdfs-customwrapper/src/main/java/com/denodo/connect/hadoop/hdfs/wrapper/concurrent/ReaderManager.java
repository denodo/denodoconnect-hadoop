/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2019, denodo technologies (http://www.denodo.com)
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

import java.util.Collection;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class ReaderManager {

    private static final ReaderManager instance = new ReaderManager();

    private CompletionService completionService;


    private ReaderManager() {

        final int size = 16;
        final ExecutorService threadPool = Executors.newFixedThreadPool(size);
        this.completionService = new ExecutorCompletionService(threadPool);

        // graceful shutdown when VDP stops
        Runtime.getRuntime().addShutdownHook(new Thread(threadPool::shutdownNow));
    }

    public static ReaderManager getInstance() {
        return instance;
    }


    public void execute(final Collection<ReaderTask> readers) throws InterruptedException, ExecutionException {

        for (final ReaderTask reader : readers) {
            this.completionService.submit(reader, null);
        }

        final int n = readers.size();
        for (int i = 0; i < n; ++i) {
            this.completionService.take().get();
        }

    }

}