/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2015, denodo technologies (http://www.denodo.com)
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

import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringUtils;

public class CSVConfig {
    
    
    private final Character separator;
    private final Character quote; 
    private final Character commentMarker; 
    private final Character escape; 
    private final boolean ignoreSpaces;
    private final boolean header;
    private final String nullValue;
    


    public CSVConfig(final String separator, final String quote, final String commentMarker, final String escape, final boolean ignoreSpaces,
            final boolean header, final String nullValue) {

        this.separator = handleInvisibleChars(separator);
        this.quote = CharUtils.toCharacterObject(quote);
        this.commentMarker = CharUtils.toCharacterObject(commentMarker);
        this.escape = CharUtils.toCharacterObject(escape);
        this.ignoreSpaces = ignoreSpaces;
        this.header = header;
        this.nullValue = nullValue;
    }

    public boolean isSeparator() {
        return this.separator != null;
    }

    public Character getSeparator() {
        return this.separator;
    }

    public boolean isQuote() {
        return this.quote != null;
    }

    public Character getQuote() {
        return this.quote;
    }

    public boolean isCommentMarker() {
        return this.commentMarker != null;
    }

    public Character getCommentMarker() {
        return this.commentMarker;
    }

    public boolean isEscape() {
        return this.escape != null;
    }
    
    public Character getEscape() {
        return this.escape;
    }

    public boolean isIgnoreSpaces() {
        return this.ignoreSpaces;
    }

    public boolean isHeader() {
        return this.header;
    }
    
    public String getNullValue() {
        return this.nullValue;
    }
    
    private Character handleInvisibleChars(final String sep) {
        
        if (StringUtils.isEmpty(sep)) {
            return null;
        }
        
        final char c;
        switch (sep) {
        case "\\t":
            c = '\t';
            break;
        case "\\n":
            c = '\n';
            break;
        case "\\r":
            c = '\r';
            break;
        case "\\f":
            c = '\f';
            break;

        default:
            c = sep.charAt(0);
        }
        
        return Character.valueOf(c);
    }
    
}
