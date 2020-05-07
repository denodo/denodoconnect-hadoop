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
package com.denodo.connect.dfs.reader;

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

import com.denodo.connect.dfs.util.type.AvroTypeUtils;
import com.denodo.connect.dfs.commons.schema.SchemaElement;
import com.denodo.connect.dfs.util.schema.AvroSchemaUtils;


public class DFSAvroFileReader extends AbstractDFSFileReader {

    private Schema schema;
    private DataFileReader<Object> dataFileReader;

    public DFSAvroFileReader(final Configuration conf, final Path path, final String fileNamePattern, final Schema schema,
        final String user, final boolean includePathColumn) throws IOException, InterruptedException {

        super(conf, path, fileNamePattern, user, includePathColumn );
        this.schema = schema;
    }

    @Override
    public void doOpenReader(final FileSystem fileSystem, final Path path,
        final Configuration configuration) throws IOException {

        final FsInput inputFile = new FsInput(path, configuration);
        final DatumReader<Object> reader = new GenericDatumReader<>(this.schema);
        this.dataFileReader = new DataFileReader<>(inputFile, reader);

    }

    public static SchemaElement getSchema(final Schema schema) {
        return AvroSchemaUtils.buildSchema(schema, schema.getName());
    }

    @Override
    public Object doRead() {

        if (this.dataFileReader.hasNext()) {
            final Object data = this.dataFileReader.next();
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

    private static Object read(final Schema schema, final Object datum) {

        Object result = null;

        final Type schemaType = schema.getType();
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
        } else if (AvroTypeUtils.isFixed(schemaType)) {
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
    private static Object readSimple(final Schema schema, final Object datum) {

        Object result = null;
        switch (schema.getType()) {
            case STRING:
                result = readString(datum);
                break;

            case BYTES:

                final ByteBuffer bb = (ByteBuffer) datum;
                final int size = bb.remaining();
                final byte[] buf = new byte[size];
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

    private static Object readString(final Object datum) {

        final Object result;
        if (datum instanceof Utf8) {
            result = datum.toString();
        } else {
            result = datum;
        }

        return result;
    }

    private static Object readRecord(final Schema schema, final Object datum) {

        final Record avroRecord = (Record) datum;
        final Object[] vdpRecord = new Object[schema.getFields().size()];
        int i = 0;
        for (final Field f : schema.getFields()) {
            vdpRecord[i++] = read(f.schema(), avroRecord.get(f.name()));
        }

        return vdpRecord;
    }

    private static Object readArray(final Schema schema, final Object datum) {

        @SuppressWarnings("unchecked")
        final
        Array<Object> avroArray = (Array<Object>) datum;
        final Object[][] vdpArray = new Object[avroArray.size()][1];
        int i = 0;
        for (final Object element : avroArray) {
            vdpArray[i++][0] = read(schema.getElementType(), element);
        }

        return vdpArray;

    }

    private static Object readUnion(final Schema schema, final Object datum) {

        final Schema nonNullSchema = AvroTypeUtils.getNotNull(schema.getTypes());
        return read(nonNullSchema, datum);
    }

    private static Object readMap(final Schema schema, final Object datum) {

        @SuppressWarnings("unchecked")
        final
        Map<String, Object> avroMap = (Map<String, Object>) datum;
        final Object[][] vdpMap = new Object[avroMap.size()][2]; // "key" and "value"
        int i = 0;
        for (final Entry<String, Object> entry : avroMap.entrySet()) {
            vdpMap[i][0] = readString(entry.getKey());
            vdpMap[i][1] = read(schema.getValueType(), entry.getValue());
            i++;
        }

        return vdpMap;
    }

    private static Object readFixed(final Object datum) {

        final Fixed avroFixed = (Fixed) datum;
        return avroFixed.bytes();
    }

}
