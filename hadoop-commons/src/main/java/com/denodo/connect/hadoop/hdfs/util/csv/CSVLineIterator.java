/*
 *
 * Copyright (c) 2020. DENODO Technologies.
 * http://www.denodo.com
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of DENODO
 * Technologies ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with DENODO.
 *
 */
package com.denodo.connect.hadoop.hdfs.util.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CSVLineIterator implements Iterator {

    private final BufferedReader bufferedReader;
    private String cachedLine;
    private boolean finished = false;

    public CSVLineIterator(final Reader reader) throws IllegalArgumentException {
        if (reader == null) {
            throw new IllegalArgumentException("Reader must not be null");
        } if (reader instanceof BufferedReader) {
            bufferedReader = (BufferedReader) reader;
        } else {
            bufferedReader = new BufferedReader(reader);
        }
    }

    @Override
    public boolean hasNext() {
        if (cachedLine != null) {
            return  true;
        } else if (finished) {
            return false;
        } else {
            try {
                while (true) {
                    String line = bufferedReader.readLine();
                    if (line == null) {
                        finished = true;
                        return false;
                    } else if (isValidLine(line)) {
                        cachedLine = line;
                        return true;
                    }
                }
            } catch(IOException e) {
                close();
                throw new IllegalStateException(e.toString());
            }
        }
    }

    protected boolean isValidLine(String line) {
        return true;
    }

    @Override
    public Object next() {
        return nextLine();
    }

    public String nextLine() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more lines");
        }
        String currentLine = cachedLine;
        cachedLine = null;
        return currentLine;
    }

    public void close() {
        finished = true;
        cachedLine = null;
        try {
            bufferedReader.close();
        } catch (IOException e) {
            throw new IllegalStateException(e.toString());
        }
    }

    public void remove() {
        throw new UnsupportedOperationException("Remove unsupported on LineIterator");
    }
}
