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

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class ColumnGroupReadingStructure {

    private int queueSize;
    private int chunkSize;

    private volatile boolean noMoreData;
    private volatile boolean finished;
    private ArrayBlockingQueue<Object[][]> data;
    private ArrayBlockingQueue<Object[][]> buffers;


    public ColumnGroupReadingStructure(final int chunkSize, final int queueSize) {
        super();

        this.queueSize = queueSize;
        this.chunkSize = chunkSize;
        this.noMoreData = false;
        this.finished = false;

        this.buffers = new ArrayBlockingQueue<>(this.queueSize);
        this.data = new ArrayBlockingQueue<>(this.queueSize);

        for (int i = 0 ; i < this.queueSize; i++) {
            final Object[][] buffer = new Object[this.chunkSize][];
            this.buffers.add(buffer);
        }

    }

    public void signalNoMoreData() {
        this.noMoreData = true;
    }

    public boolean isFinished() {
        if (this.finished) {
            return true;
        }
        if (!this.noMoreData) {
            return false;
        }
        if (this.data.isEmpty()) {
            this.finished = true;
            return true;
        }
        return false;
    }


    public Object[][] acquireBufferChunk() throws InterruptedException {
        return this.buffers.poll(100, TimeUnit.MILLISECONDS);
    }

    public boolean returnDataChunk(final Object[][] chunk) throws InterruptedException {
        Arrays.fill(chunk, null);
        return this.buffers.offer(chunk, 100, TimeUnit.MILLISECONDS);
    }

    public Object[][] acquireDataChunk() throws InterruptedException {
        return this.data.poll(100, TimeUnit.MILLISECONDS);
    }

    public boolean publishDataChunk(final Object[][] chunk) throws InterruptedException {
        return this.data.offer(chunk, 100, TimeUnit.MILLISECONDS);
    }



}