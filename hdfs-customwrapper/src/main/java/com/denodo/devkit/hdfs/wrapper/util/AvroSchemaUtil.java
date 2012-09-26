package com.denodo.devkit.hdfs.wrapper.util;

import java.sql.Types;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.denodo.devkit.hdfs.wrapper.exceptions.UnsupportedTypeException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;

public class AvroSchemaUtil {
    @SuppressWarnings("rawtypes")
    public static CustomWrapperSchemaParameter createSchemaParameter(
            Schema schema, String schema_name) throws UnsupportedTypeException,
            CustomWrapperException {

        boolean isSearchable = true;
        boolean isUpdeatable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        Type schema_type = schema.getType();
        if (isSimple(schema_type)) {
            return new CustomWrapperSchemaParameter(schema_name,
                    mapAvroSimpleType(schema.getType()), null, isSearchable,
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT,
                    isUpdeatable, isNullable, !isMandatory);
        }

        else if (isEnum(schema_type)) {
            return new CustomWrapperSchemaParameter(
                    schema_name,
                    java.sql.Types.ARRAY,
                    new CustomWrapperSchemaParameter[] { new CustomWrapperSchemaParameter(
                            "symbols", java.sql.Types.VARCHAR) }, isSearchable,
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT,
                    isUpdeatable, isNullable, !isMandatory);
        } else if (isArray(schema_type)) {
            Schema array_element = schema.getElementType();
            return new CustomWrapperSchemaParameter(schema_name,
                    java.sql.Types.ARRAY,
                    new CustomWrapperSchemaParameter[] { createSchemaParameter(
                            array_element, array_element.getName()) },
                    isSearchable,
                    CustomWrapperSchemaParameter.ASC_AND_DESC_SORT,
                    isUpdeatable, isNullable, !isMandatory);
        } else if (isUnion(schema_type)) {
            // Currently we only support UNION types with two types, being one
            // of them NULL
            // TODO Support all UNION types
            List<Schema> schemas = schema.getTypes();
            if (schemas.size() > 2)
                throw new UnsupportedTypeException(schema_type.name()
                        + " containing more than two schemas ");
            else if (!containsNull(schemas))
                throw new UnsupportedTypeException(schema_type.name()
                        + " without schema NULL  ");
            Schema notNullSchema = getNotNull(schemas);
            if (notNullSchema != null)
                return createSchemaParameter(notNullSchema, schema_name);
            else
                throw new CustomWrapperException("This should never happen");
        } else if (isRecord(schema_type)) {
            String recordName = schema.getName();
            CustomWrapperSchemaParameter[] recordFields = new CustomWrapperSchemaParameter[schema
                    .getFields().size()];
            int i = 0;
            for (Field f : schema.getFields()) {
                recordFields[i] = createSchemaParameter(f.schema(), f.name());
                i++;
            }
            return new CustomWrapperSchemaParameter(recordName, Types.STRUCT,
                    recordFields, !isSearchable,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, isUpdeatable,
                    false, !isMandatory);
        } else
            throw new UnsupportedTypeException(schema_type.name());
    }

    /**
     * Returns first not null schema present in the list. Null otherwise
     * 
     * @param schemas
     *            list of schemas
     * @return first not null schema present in the list. Null otherwise
     */
    private static Schema getNotNull(List<Schema> schemas) {
        for (Schema s : schemas)
            if (!s.getType().equals(Type.NULL))
                return s;
        return null;
    }

    /**
     * Returns true if the passed field is array type
     * 
     * @param field
     * @return true if the passed field is array type
     */
    private static boolean isArray(Type type) {
        return type.equals(Type.ARRAY);
    }

    /**
     * Return true if the passed field is enum type
     * 
     * @param field
     * @return true if the passed field is enum type
     */
    private static boolean isEnum(Type type) {
        return type.equals(Type.ENUM);
    }

    /**
     * Return true if the passed field is union type
     * 
     * @param field
     * @return true if the passed field is union type
     */
    private static boolean isUnion(Type type) {
        return type.equals(Type.UNION);
    }

    /**
     * Return true if the passed field is record type
     * 
     * @param field
     * @return true if the passed field is record type
     */
    private static boolean isRecord(Type type) {
        return type.equals(Type.RECORD);
    }

    /**
     * Return true if the passed field is simple type
     * 
     * @param field
     * @return true if the passed field is simple type
     */
    private static boolean isSimple(Type type) {
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
     * @param fieldType
     *            the Avro <code>FieldType</code>
     * @return a type code, as defined in <code>java.sql.Types</code>
     */
    private static int mapAvroSimpleType(Schema.Type type) {
        if (type.equals(Type.BOOLEAN)) {
            return java.sql.Types.BOOLEAN;
        } else if (type.equals(Type.BYTES)) {
            return java.sql.Types.VARBINARY;
        } else if (type.equals(Type.DOUBLE)) {
            return java.sql.Types.DOUBLE;
        } else if (type.equals(Type.FIXED)) {
            return java.sql.Types.VARCHAR;
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
        for (Schema s : schemas)
            if (s.getType().equals(Type.NULL))
                return true;
        return false;
    }

}
