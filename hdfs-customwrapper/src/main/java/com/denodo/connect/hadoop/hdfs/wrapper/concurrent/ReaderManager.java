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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class ReaderManager {

    private static final ReaderManager instance = new ReaderManager();

    private int parallelism;
    private CompletionService<Void> completionService;


    private ReaderManager() {

        this.parallelism = computeParallelism();
        final int poolSize = computePoolSize(this.parallelism);
        final ExecutorService threadPool = Executors.newFixedThreadPool(poolSize);
        this.completionService = new ExecutorCompletionService<>(threadPool);

        // graceful shutdown when VDP stops
        Runtime.getRuntime().addShutdownHook(new Thread(threadPool::shutdownNow));
    }

    public static ReaderManager getInstance() {
        return instance;
    }

    public void execute(final Collection<ReaderTask> readers) throws ExecutionException {

        final Collection<Future<Void>> futures = new ArrayList<>(readers.size());
        for (final ReaderTask reader : readers) {
            futures.add(this.completionService.submit(reader));
        }

        int n = readers.size();
        try {
            while (n > 0) {
                n --;
                this.completionService.take().get();
            }
        } catch (final InterruptedException e) {
            cancel(futures, n);
        } catch (final ExecutionException e) {
            cancel(futures, n);

            throw e;
        }


    }

    private void cancel(final Collection<Future<Void>> futures, final int remainingTasks) {

        for (final Future<Void> f : futures) {
            f.cancel(true);
        }

        for (int i = 0; i < remainingTasks; i ++) {
            try {
                this.completionService.take();  // remove interrupted tasks from the queue
            } catch (final InterruptedException ignore) {

            }
        }
    }

    private static int computeParallelism() {

        int parallelism = Runtime.getRuntime().availableProcessors() - 1;
        if (parallelism <= 0) {
            parallelism = 1;
        }

        return parallelism;
    }

    private static int computePoolSize(final int parallelism) {
        return parallelism * 2;
    }

    public int getParallelism() {
        return this.parallelism;
    }

}