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

    private static final int DEFAULT_POOL_SIZE = 20;

    private ThreadPoolExecutor threadPool;


    public ReaderManager(final int threadPoolSize) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("AVAILABLE PROCESSORS " + Runtime.getRuntime().availableProcessors());
        }
        final int poolSize = threadPoolSize > 0 ? threadPoolSize : DEFAULT_POOL_SIZE;
        this.threadPool = new ThreadPoolExecutor(0, // try to reduce the pool size to 0 if threads are idle long enough
            poolSize, 60L, TimeUnit.SECONDS, // terminate thread after 60s idle
            new SynchronousQueue<Runnable>());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created thread pool " + this.threadPool);
        }

        // graceful shutdown when VDP stops
        Runtime.getRuntime().addShutdownHook(new Thread(this.threadPool::shutdownNow));
    }

    public void resizePool(final int threadPoolSize) {

        if (threadPoolSize > 0 && this.threadPool.getMaximumPoolSize() != threadPoolSize) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Resizing thread pool " + this.threadPool + " to: " + threadPoolSize);
            }
            this.threadPool.setMaximumPoolSize(threadPoolSize);
        }

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


}