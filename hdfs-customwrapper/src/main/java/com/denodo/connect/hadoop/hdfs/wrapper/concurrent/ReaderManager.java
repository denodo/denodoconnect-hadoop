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
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReaderManager {

    private static final Logger LOG = LoggerFactory.getLogger(ReaderManager.class);

    private static final ReaderManager instance = new ReaderManager();

   // private int parallelism;
    private ExecutorService threadPool;


    private ReaderManager() {

       // this.parallelism = computeParallelism();
        if (LOG.isDebugEnabled()) {
            LOG.debug("AVAILABLE PROCESSORS " + Runtime.getRuntime().availableProcessors());
        }
        final int poolSize = 20;//computePoolSize(this.parallelism);
        this.threadPool = Executors.newFixedThreadPool(poolSize);

        // graceful shutdown when VDP stops
        Runtime.getRuntime().addShutdownHook(new Thread(this.threadPool::shutdownNow));
    }

    public static ReaderManager getInstance() {
        return instance;
    }

    public void execute(final Collection<Callable> readers) throws ExecutionException {

        final Collection<Future<Void>> futures = new ArrayList<>(readers.size());
        for (final Callable reader : readers) {
            futures.add(this.threadPool.submit(reader));
        }

        try {
            for (final Future<Void> future : futures) {
                future.get();
            }
        } catch (final InterruptedException e) {
            cancel(futures);
        } catch (final ExecutionException e) {
            cancel(futures);

            throw e;
        }


    }

    private void cancel(final Collection<Future<Void>> futures) {

        for (final Future<Void> f : futures) {
            f.cancel(true);
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

   // public int getParallelism() {
   //     return this.parallelism;
   // }

}