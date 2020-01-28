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
package com.denodo.connect.hadoop.hdfs.util.type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

public final class ParquetTypeUtils {

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);


    private ParquetTypeUtils() {

    }

    public static Class<?> toJava(final Type type) {
        if(type.isPrimitive()){
            final PrimitiveTypeName primitiveTypeName= type.asPrimitiveType().getPrimitiveTypeName();
            if(primitiveTypeName.equals(PrimitiveTypeName.BINARY)) {
                if(type.getOriginalType()!=null){
                    if(type.getOriginalType().equals(OriginalType.UTF8)){
                        return String.class;
                    } if(type.getOriginalType().equals(OriginalType.JSON)){
                        return String.class;
                    } if(type.getOriginalType().equals(OriginalType.BSON)){
                        return String.class;
                    }

                }
                return ByteBuffer.class; 

            }else if(primitiveTypeName.equals(PrimitiveTypeName.BOOLEAN)) {
                return Boolean.class;
            }else if(primitiveTypeName.equals(PrimitiveTypeName.DOUBLE)) {
                return Double.class;
            }else if(primitiveTypeName.equals(PrimitiveTypeName.FLOAT)) {
                return Float.class;
            }else if(primitiveTypeName.equals(PrimitiveTypeName.INT32)) {
                if (OriginalType.DECIMAL.equals(type.getOriginalType())) {
                    return java.math.BigDecimal.class;
                } else if (OriginalType.DATE.equals(type.getOriginalType())) {
                    return java.util.Date.class;
                }
                return Integer.class;
            }else if(primitiveTypeName.equals(PrimitiveTypeName.INT64)) {
                //we dont differentiate INT64 from TIMESTAMP_MILLIS original types
                return Long.class;
            }else if(primitiveTypeName.equals(PrimitiveTypeName.INT96)) {
                // As stated in PARQUET-323, Parquet `INT96` was originally introduced to represent nanosecond
                // timestamp in Impala for some historical reasons.  It's not recommended to be used for any
                // other types and will probably be deprecated in some future version of parquet-format spec.
                return java.sql.Timestamp.class;
            } else if(primitiveTypeName.equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
                if (OriginalType.DECIMAL.equals(type.getOriginalType())) {
                    return java.math.BigDecimal.class;
                }
                return byte[].class;
            }else{
                throw new IllegalArgumentException("Unknown type: " + type.getName());
            }
        }
        throw new IllegalArgumentException("Type of the field "+ type.toString()+", does not supported by the custom warpper ");

    }
    
    /**
     * This method check if the field is a group
     */
    public static boolean isGroup(final Type field) {
        return !field.asGroupType().getFields().isEmpty() && field.getOriginalType() == null;

    }
    
    /**
     * This method check if the field is a Map
     */
    public static boolean isMap(final Type field) {
        return !field.asGroupType().getFields().isEmpty() && field.getOriginalType() != null
                && field.getOriginalType().equals(OriginalType.MAP);

    }

    /**
     * This method check if the field is a List
     */
    public static boolean isList(final Type field) {
        return !field.asGroupType().getFields().isEmpty() && field.getOriginalType() != null
                && field.getOriginalType().equals(OriginalType.LIST);

    }

    /**
     * Returns GMT timestamp from binary encoded parquet timestamp (INT96).
     *
     *  (deprecated) Timestamps saved as an INT96 are made up of the nanoseconds in the day (first 8 byte) and the Julian day
     *  (last 4 bytes). No timezone is attached to this value. To convert the timestamp into nanoseconds since the Unix
     *  epoch, 00:00:00.000000 on 1 January 1970, the following formula can be used: (julian_day - 2440588) * (86400 * 1000 *
     *  1000 * 1000) + nanoseconds. The magic number 2440588 is the julian day for 1 January 1970.
     *
     * @param bytes INT96 parquet timestamp
     * @return timestamp in millis, GMT timezone
     */
    public static long int96ToTimestampMillis(final Binary bytes) {
        final ByteBuffer buf = bytes.toByteBuffer();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        final long timeOfDayNanos = buf.getLong();
        final int julianDay = buf.getInt();

        return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    }

    private static long julianDayToMillis(final int julianDay) {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }
}



