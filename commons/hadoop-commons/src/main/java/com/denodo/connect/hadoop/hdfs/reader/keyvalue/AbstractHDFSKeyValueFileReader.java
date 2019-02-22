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
package com.denodo.connect.hadoop.hdfs.reader.keyvalue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.AbstractHDFSFileReader;
import com.denodo.connect.hadoop.hdfs.util.configuration.InitUtils;
import com.denodo.connect.hadoop.hdfs.util.type.TypeUtils;

public abstract class AbstractHDFSKeyValueFileReader extends AbstractHDFSFileReader {

    private String hadoopKeyClass;
    private String hadoopValueClass;

    private Writable key;
    private Writable value;


    public AbstractHDFSKeyValueFileReader(final Configuration configuration, final String hadoopKeyClass,
            final String hadoopValueClass, final Path outputPath, final String fileNamePattern, final String user)
            throws IOException, InterruptedException {

        super(configuration, outputPath, fileNamePattern, user);

        this.hadoopKeyClass = hadoopKeyClass;
        this.hadoopValueClass = hadoopValueClass;
        this.key = getInitKey(hadoopKeyClass, configuration);
        this.value = getInitValue(hadoopValueClass, configuration);
    }

    public static Collection<SchemaElement> getSchema(final String hadoopKeyClass, final String hadoopValueClass) {

        final Collection<SchemaElement> schema = new ArrayList<SchemaElement>();

        final Class<?> keyJavaClass = TypeUtils.toJava(hadoopKeyClass);
        final SchemaElement keyElement = new SchemaElement(Parameter.KEY, keyJavaClass);
        schema.add(keyElement);
        final Class<?> valueJavaClass = TypeUtils.toJava(hadoopValueClass);
        final SchemaElement valueElement = new SchemaElement(Parameter.VALUE, valueJavaClass);
        if (valueJavaClass.equals(List.class)) {
            final Class<?> subValueJavaClass = TypeUtils.toJava(StringUtils.substringBeforeLast(hadoopValueClass, "[]"));
            valueElement.add(new SchemaElement(Parameter.VALUE, subValueJavaClass));
        }
        schema.add(valueElement);

        return schema;
    }

    /**
     * Reads the next key-value pair and stores it in the key and value parameters.
     */
    @Override
    public Object doRead() throws IOException {

        if (doRead(this.key, this.value)) {
            // 'key' and 'value' are filled with their values
            return getValue(this.key, this.value);
        }

        return null;

    }

    /**
     * @return an instance of the key class initialized (necessary
     * to read output).
     */
    private static Writable getInitKey(final String hadoopKeyClass, final Configuration configuration) {
        return InitUtils.getInitKey(hadoopKeyClass, configuration);
    }

    /**
     * @return an instance of the value class initialized (necessary
     * to read output).
     */
    private static Writable getInitValue(final String hadoopValueClass, final Configuration configuration) {
        return InitUtils.getInitValue(hadoopValueClass, configuration);
    }

    private Object getValue(final Writable k, final Writable v) {

        final Object[] data = new Object[2];
        data[0] = TypeUtils.getValue(this.hadoopKeyClass, k);
        data[1] = TypeUtils.getValue(this.hadoopValueClass, v);

        return data;
    }

    public abstract <K extends Writable, V extends Writable> boolean doRead(
        K key, V value) throws IOException;

}
