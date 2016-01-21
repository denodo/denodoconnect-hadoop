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
package com.denodo.connect.hadoop.hdfs.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.schema.AvroSchemaUtils;
import com.denodo.connect.hadoop.hdfs.util.type.AvroTypeUtils;


public class HDFSAvroFileReader extends AbstractHDFSFileReader {

    private Schema schema;
    private DataFileReader<Object> dataFileReader;

    public HDFSAvroFileReader(Configuration conf, Path path, Schema schema,
        String user) throws IOException, InterruptedException {

        super(conf, path, user);
        this.schema = schema;
    }

    @Override
    public void openReader(FileSystem fileSystem, Path path,
        Configuration configuration) throws IOException {

        if (isFile(path)) {
            FsInput inputFile = new FsInput(path, configuration);
            DatumReader<Object> reader = new GenericDatumReader<Object>(this.schema);
            this.dataFileReader = new DataFileReader<Object>(inputFile, reader);
        } else {
            throw new IllegalArgumentException("'" + path + "' is not a file");
        }
    }

    public static SchemaElement getSchema(Schema schema) {
        return AvroSchemaUtils.buildSchema(schema, schema.getName());
    }

    @Override
    public Object doRead() {

        if (this.dataFileReader.hasNext()) {
            Object data = this.dataFileReader.next();
            return read(this.schema, data);
        }

        return null;
    }

    @Override
    public void closeReader() throws IOException {
        if (this.dataFileReader != null) {
            this.dataFileReader.close();
        }
    }

    private static Object read(Schema schema, Object datum) {

        Object result = null;

        Type schemaType = schema.getType();
        if (AvroTypeUtils.isSimple(schemaType)) {
            result = readSimple(schema, datum);
        } else if (AvroTypeUtils.isEnum(schemaType)) {
            result = readString(datum);
        } else if (AvroTypeUtils.isArray(schemaType)) {
            result = readArray(schema, datum);
        } else if (AvroTypeUtils.isUnion(schemaType)) {
            result = readUnion(schema, datum);
        } else if (AvroTypeUtils.isRecord(schemaType)) {
            result = readRecord(schema, datum);
        } else if (AvroTypeUtils.isMap(schemaType)) {
            result = readMap(schema, datum);
        }  else if (AvroTypeUtils.isFixed(schemaType)) {
            result = readFixed(datum);
        }

        return result;
    }

    /**
     * Avro Type --- Generic Java mapping
     *
     *  null            null type
     *  boolean         boolean
     *  int             int
     *  long            long
     *  float           float
     *  double          double
     *  bytes           java.nio.ByteBuffer
     *  string          org.apache.avro.util.Utf8 or java.lang.String
     *
     */
    private static Object readSimple(Schema schema, Object datum) {

        Object result = null;
        switch (schema.getType()) {
            case STRING:
                result = readString(datum);
                break;

            case BYTES:

                ByteBuffer bb = (ByteBuffer) datum;
                int size = bb.remaining();
                byte[] buf = new byte[size];
                bb.get(buf, 0, size);
                result = buf;
                break;

            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case NULL:
            default:
                result = datum;
        }

        return result;
    }

    private static Object readString(Object datum) {

        Object result;
        if (datum instanceof Utf8) {
            result = datum.toString();
        } else {
            result = datum;
        }

        return result;
    }

    private static Object readRecord(Schema schema, Object datum) {

        Record avroRecord = (Record) datum;
        Object[] vdpRecord = new Object[schema.getFields().size()];
        int i = 0;
        for (Field f : schema.getFields()) {
            vdpRecord[i++] = read(f.schema(), avroRecord.get(f.name()));
        }

        return vdpRecord;
    }

    private static Object readArray(Schema schema, Object datum) {

        @SuppressWarnings("unchecked")
        Array<Object> avroArray = (Array<Object>) datum;
        Object[][] vdpArray = new Object[avroArray.size()][1];
        int i = 0;
        for (Object element : avroArray) {
            vdpArray[i++][0] = read(schema.getElementType(), element);
        }

        return vdpArray;

    }

    private static Object readUnion(Schema schema, Object datum) {

        Schema nonNullSchema = AvroTypeUtils.getNotNull(schema.getTypes());
        return read(nonNullSchema, datum);
    }

    private static Object readMap(Schema schema, Object datum) {

        @SuppressWarnings("unchecked")
        Map<String, Object> avroMap = (Map<String, Object>) datum;
        Object[][] vdpMap = new Object[avroMap.size()][2]; // "key" and "value"
        int i = 0;
        for (Entry<String, Object> entry : avroMap.entrySet()) {
            vdpMap[i][0] = readString(entry.getKey());
            vdpMap[i][1] = read(schema.getValueType(), entry.getValue());
            i++;
        }

        return vdpMap;
    }

    private static Object readFixed(Object datum) {

        Fixed avroFixed = (Fixed) datum;
        return avroFixed.bytes();
    }

}
