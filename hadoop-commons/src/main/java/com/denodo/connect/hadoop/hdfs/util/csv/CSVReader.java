/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2014-2015, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.hdfs.util.csv;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.denodo.connect.hadoop.hdfs.commons.exception.InternalErrorException;


/**
 * 
 * Reader for reading comma separated files. Default format is <a href="http://tools.ietf.org/html/rfc4180">RFC 4180</a>
 */
public class CSVReader implements Iterator<List<String>> {

    private CSVParser parser;
    private Iterator<CSVRecord> iterator;
    
    public CSVReader(final Reader reader, final CSVConfig csvConfig) throws IOException {

        this.parser = getFormat(csvConfig).parse(reader);
        this.iterator = this.parser.iterator();
        if (!hasNext()) {
            throw new InternalErrorException("Empty delimited file.");
        }
        
    }
    
    private CSVFormat getFormat(final CSVConfig config) {
        
        CSVFormat format = CSVFormat.RFC4180;
        if (config.isSeparator()) {
            format = format.withDelimiter(config.getSeparator());
        }

        if (config.isQuote()) {
            format = format.withQuote(config.getQuote());
        }
        
        if (config.isCommentMarker()) {
            format = format.withCommentMarker(config.getCommentMarker());
        }        
        
        if (config.isEscape()) {
            format = format.withEscape(config.getEscape());
        }
        
        format = format.withIgnoreSurroundingSpaces(config.isIgnoreSpaces());
        format = format.withSkipHeaderRecord(!config.isHeader());
        format = format.withNullString(config.getNullValue());
        
        return format;
    }
    
    @Override
    public boolean hasNext() {
        try {
            return this.iterator.hasNext();
        } catch (final Exception e) {
            close();
            throw new InternalErrorException("Error accessing delimited data", e); 
        }
    }
    
    /**
     * Each element of the iterator is a List of Strings containing the values from the next row of the CSV data. When the
     * returned result is the last element the iterator closes the reader.
     * 
     * @return a List of String. First result returned contains the column names.
     */
    @Override
    public List<String> next() {
        
        try {
            return toList(this.iterator.next());
        } catch (final Exception e) {
            close();
            throw new InternalErrorException("Error accessing delimited data", e); 
        } finally {
            if (!hasNext()) {
                close();
            }
        }
    }
    
    public void close() {
        
        try {
            this.parser.close();
        } catch (final IOException e) {
            // ignore
        }
    }
    
    private static List<String> toList(final CSVRecord record) {
        
        final List<String> asList = new ArrayList<>(record.size());
        for (final String item : record) {
            asList.add(item);
        }
        
        return asList;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();        
    }
}
