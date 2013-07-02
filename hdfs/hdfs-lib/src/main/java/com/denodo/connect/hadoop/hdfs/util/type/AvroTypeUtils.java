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
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public final class AvroTypeUtils {

    private static final Schema NULL_SCHEMA = Schema.create(Type.NULL);


    private AvroTypeUtils() {

    }

    public static Class<?> toJava(Schema schema) {

        switch (schema.getType()) {
            case FIXED:   return ByteBuffer.class;
            case RECORD:  return Object.class;
            case MAP:     return Map.class;
            case ARRAY:   return List.class;
            case UNION:
                List<Schema> types = schema.getTypes();
                if ((types.size() == 2) && types.contains(NULL_SCHEMA)) {
                    return toJava(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
                }
                throw new IllegalArgumentException("Only unions with two types are allowed and one type of the union must be NULL: " + schema);
            case ENUM:
            case STRING:  return String.class;
            case BYTES:   return ByteBuffer.class;
            case INT:     return Integer.class;
            case LONG:    return Long.class;
            case FLOAT:   return Float.class;
            case DOUBLE:  return Double.class;
            case BOOLEAN: return Boolean.class;
            case NULL:    return Void.class;
            default: throw new IllegalArgumentException("Unknown type: " + schema);
        }
    }

    /**
     * Returns the first not null schema in the list. Null otherwise
     */
    public static Schema getNotNull(List<Schema> schemas) {

        for (Schema s : schemas) {
            if (!(Type.NULL).equals(s.getType())) {
                return s;
            }
        }
        return null;
    }

    public static boolean isArray(Type type) {
        return type.equals(Type.ARRAY);
    }

    public static boolean isEnum(Type type) {
        return type.equals(Type.ENUM);
    }

    public static boolean isFixed(Type type) {
        return type.equals(Type.FIXED);
    }

    public static boolean isMap(Type type) {
        return type.equals(Type.MAP);
    }

    public static boolean isUnion(Type type) {
        return type.equals(Type.UNION);
    }

    public static boolean isRecord(Type type) {
        return type.equals(Type.RECORD);
    }

    public static boolean isSimple(Type type) {
        return !(type.equals(Type.ARRAY) || type.equals(Type.ENUM)
            || type.equals(Type.RECORD) || type.equals(Type.MAP)
            || type.equals(Type.UNION) || type.equals(Type.FIXED));
    }

}
