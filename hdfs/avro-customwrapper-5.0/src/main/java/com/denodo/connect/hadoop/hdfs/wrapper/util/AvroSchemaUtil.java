/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2012, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.hdfs.wrapper.util;

import java.sql.Types;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;

public final class AvroSchemaUtil {

    private AvroSchemaUtil() {

    }

    public static CustomWrapperSchemaParameter createSchemaParameter(
        Schema schema, String schemaName) throws CustomWrapperException {

        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        Type schemaType = schema.getType();
        if (isSimple(schemaType)) {
            return new CustomWrapperSchemaParameter(schemaName,
                getSQLType(schema.getType()), null, isSearchable,
                CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, isUpdateable,
                isNullable, !isMandatory);
        } else if (isEnum(schemaType)) {
            return new CustomWrapperSchemaParameter(schemaName,
                Types.VARCHAR, null, isSearchable,
                CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, isUpdateable,
                isNullable, !isMandatory);
        } else if (isArray(schemaType)) {
            Schema arrayElement = schema.getElementType();
            return new CustomWrapperSchemaParameter(schemaName, Types.ARRAY,
                new CustomWrapperSchemaParameter[] { createSchemaParameter(
                    arrayElement, arrayElement.getName()) }, isSearchable,
                CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, isUpdateable,
                isNullable, !isMandatory);
        } else if (isUnion(schemaType)) {
            // Only support UNION types with two types, being one of them NULL
            List<Schema> schemas = schema.getTypes();
            if (schemas.size() > 2) {
                throw new CustomWrapperException("Invalid type for :" + schemaType.name()
                    + ". Only unions with two types are allowed.");
            } else if (!containsNull(schemas)) {
                throw new CustomWrapperException("Invalid type for :" + schemaType.name()
                    + ". One type of the union must be NULL.");
            }
            Schema notNullSchema = getNotNull(schemas);
            if (notNullSchema != null) {
                return createSchemaParameter(notNullSchema, schemaName);
            }
            throw new CustomWrapperException("This should never happen");
        } else if (isRecord(schemaType)) {
            String recordName = schema.getName();
            CustomWrapperSchemaParameter[] recordFields =
                new CustomWrapperSchemaParameter[schema.getFields().size()];
            int i = 0;
            for (Field f : schema.getFields()) {
                recordFields[i] = createSchemaParameter(f.schema(), f.name());
                i++;
            }
            return new CustomWrapperSchemaParameter(recordName, Types.STRUCT,
                recordFields, !isSearchable,
                CustomWrapperSchemaParameter.NOT_SORTABLE, isUpdateable, !isNullable,
                !isMandatory);
        } else if (isMap(schemaType)) {
            // map is a struct with two fields: key and value
            CustomWrapperSchemaParameter[] mapFields =
                new CustomWrapperSchemaParameter[2];
            mapFields[0] = new CustomWrapperSchemaParameter("key",
                Types.VARCHAR, null, isSearchable,
                CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, isUpdateable,
                isNullable, !isMandatory);
            mapFields[1] = createSchemaParameter(schema.getValueType(), "value");

            return new CustomWrapperSchemaParameter(schemaName,
                Types.ARRAY, mapFields, isSearchable,
                CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, isUpdateable,
                isNullable, !isMandatory);

        } else if (isFixed(schemaType)) {
            return new CustomWrapperSchemaParameter(schemaName,
                Types.VARBINARY, null, isSearchable,
                CustomWrapperSchemaParameter.ASC_AND_DESC_SORT, isUpdateable,
                isNullable, !isMandatory);
        } else {
            throw new CustomWrapperException(schemaType.name());
        }
    }

    /**
     * Returns the first not null schema in the list. Null otherwise
     */
    public static Schema getNotNull(List<Schema> schemas) {

        for (Schema s : schemas) {
            if (!s.getType().equals(Type.NULL)) {
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

    /**
     * Obtains a type code, as defined in <code>java.sql.Types</code>,
     * equivalent to the passed Avro simple <code>Schema.Type</code>. See the
     * Avro documentation for more information (<a href="
     * http://avro.apache.org/docs/1.5.4/spec.html ">here</a> and <a href="
     * http:
     * //avro.apache.org/docs/1.5.4/api/java/org/apache/avro/Schema.Type.html" >
     * here</a>).
     *
     * @param fieldType the Avro <code>FieldType</code>
     * @return a type code, as defined in <code>java.sql.Types</code>
     */
    private static int getSQLType(Schema.Type type) {

        if (type.equals(Type.BOOLEAN)) {
            return java.sql.Types.BOOLEAN;
        } else if (type.equals(Type.BYTES)) {
            return java.sql.Types.VARBINARY;
        } else if (type.equals(Type.DOUBLE)) {
            return java.sql.Types.DOUBLE;
        } else if (type.equals(Type.FLOAT)) {
            return java.sql.Types.FLOAT;
        } else if (type.equals(Type.INT)) {
            return java.sql.Types.INTEGER;
        } else if (type.equals(Type.LONG)) {
            return java.sql.Types.BIGINT;
        } else if (type.equals(Type.NULL)) {
            return java.sql.Types.VARCHAR;
        } else if (type.equals(Type.STRING)) {
            return java.sql.Types.VARCHAR;
        }
        return java.sql.Types.VARCHAR;
    }

    private static boolean containsNull(List<Schema> schemas) {

        for (Schema s : schemas) {
            if (s.getType().equals(Type.NULL)) {
                return true;
            }
        }
        return false;
    }

}
