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

import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

public final class ParquetTypeUtils {




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
                return ByteBuffer.class;
            }else if(primitiveTypeName.equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
                return ByteBuffer.class;                
            }else{
                throw new IllegalArgumentException("Unknown type: " + type.getName());
            }
        }
        throw new IllegalArgumentException("Type of the field "+ type.toString()+", does not supported by the custom warpper ");

    }
    
    /**
     * This method check if the field is a group
     * @param field
     * @return
     */
    public static boolean isGroup(final Type field) {
        return field.asGroupType().getFields().size() > 0 && field.getOriginalType() == null;

    }
    
    /**
     * This method check if the field is a Map
     * @param valueMap
     * @return
     */
    public static boolean isMap(final Type field) {
        return field.asGroupType().getFields().size() > 0 && field.getOriginalType() != null
                && field.getOriginalType().equals(OriginalType.MAP);

    }

    /**
     * This method check if the field is a List
     * @param valueMap
     * @return
     */
    public static boolean isList(final Type field) {
        return field.asGroupType().getFields().size() > 0 && field.getOriginalType() != null
                && field.getOriginalType().equals(OriginalType.LIST);

    }
}



