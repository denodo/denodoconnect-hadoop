package com.denodo.connect.hadoop.hdfs.wrapper.util;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;


public final class AvroReaderUtil {

    private AvroReaderUtil() {

    }

    public static Object read(Schema schema, Object datum) {

        Object result = null;

        Type schemaType = schema.getType();
        if (AvroSchemaUtil.isSimple(schemaType)) {
            result = readSimple(schema, datum);
        } else if (AvroSchemaUtil.isEnum(schemaType)) {
            result = readString(datum);
        } else if (AvroSchemaUtil.isArray(schemaType)) {
            result = readArray(schema, datum);
        } else if (AvroSchemaUtil.isUnion(schemaType)) {
            result = readUnion(schema, datum);
        } else if (AvroSchemaUtil.isRecord(schemaType)) {
            result = readRecord(schema, datum);
        } else if (AvroSchemaUtil.isMap(schemaType)) {
            result = readMap(schema, datum);
        }  else if (AvroSchemaUtil.isFixed(schemaType)) {
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

        Array<Object> avroArray = (Array<Object>) datum;
        Object[][] vdpArray = new Object[avroArray.size()][1];
        int i = 0;
        for (Object element : avroArray) {
            vdpArray[i++][0] = read(schema.getElementType(), element);
        }

        return vdpArray;

    }

    private static Object readUnion(Schema schema, Object datum) {

        Schema nonNullSchema = AvroSchemaUtil.getNotNull(schema.getTypes());
        return read(nonNullSchema, datum);
    }

    private static Object readMap(Schema schema, Object datum) {

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
