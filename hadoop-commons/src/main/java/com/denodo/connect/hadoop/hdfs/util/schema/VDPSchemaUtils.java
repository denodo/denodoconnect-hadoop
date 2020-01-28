package com.denodo.connect.hadoop.hdfs.util.schema;

import java.util.Collection;

import org.apache.parquet.schema.PrimitiveType;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.type.TypeUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;


public final class VDPSchemaUtils {

    private VDPSchemaUtils() {

    }

    public static CustomWrapperSchemaParameter[] buildSchema(final Collection<SchemaElement> schema) {

        final CustomWrapperSchemaParameter[] vdpSchema = new CustomWrapperSchemaParameter[schema.size()];
        int i = 0;
        for (final SchemaElement element : schema) {
            vdpSchema[i++] = buildSchemaParameter(element);
        }

        return vdpSchema;

    }

    public static CustomWrapperSchemaParameter buildSchemaParameter(final SchemaElement element) {

        final boolean isSearchable = true;
        final boolean isUpdateable = true;
        final boolean isNullable = true;
        final boolean isMandatory = true;


        final CustomWrapperSchemaParameter[] params = new CustomWrapperSchemaParameter[element.getElements().size()];
        int i = 0;
        for (final SchemaElement e : element.getElements()) {
            params[i++] = buildSchemaParameter(e);
        }

        final int type = TypeUtils.toSQL(element.getType());
        return new CustomWrapperSchemaParameter(element.getName(), type,
            (params.length == 0) ? null : params,
            !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
            !isUpdateable, isNullable, !isMandatory);
    }

    public static CustomWrapperSchemaParameter[] buildSchemaParameter(final Collection<SchemaElement> elements) {

        final boolean isSearchable = true;
        final boolean isUpdateable = true;
        final boolean isNullable = true;
        final boolean isMandatory = true;


        final CustomWrapperSchemaParameter[] params = new CustomWrapperSchemaParameter[elements.size()];
        int i = 0;
        for (final SchemaElement e : elements) {
            params[i++] = new CustomWrapperSchemaParameter(e.getName(),TypeUtils.toSQL(e.getType()),
                    (params.length == 0) ? null : params,
                            !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                            !isUpdateable, isNullable, !isMandatory);
        }
        
        return params;
    }

    public static CustomWrapperSchemaParameter[] buildSchemaParameterParquet(final Collection<SchemaElement> elements) {

        final CustomWrapperSchemaParameter[] params = new CustomWrapperSchemaParameter[elements.size()];
        int i = 0;
        for (final SchemaElement e : elements) {
            params[i++] = buildSchemaParameterParquet(e);
        }
        return params;
    }
    
    private static CustomWrapperSchemaParameter buildSchemaParameterParquet(final SchemaElement element) {

        final boolean isSearchable = true;
        final boolean isUpdateable = true;
        final boolean isMandatory = true;

        final CustomWrapperSchemaParameter[] params = new CustomWrapperSchemaParameter[element.getElements().size()];
        int i = 0;
        for (final SchemaElement e : element.getElements()) {
            params[i++] = buildSchemaParameterParquet(e);
        }
        
        final int type = TypeUtils.toSQL(element.getType());

        //If this field have INT96 or FIXED_LEN_BYTE_ARRAY type is a deprecated timestamp and the parquet FilterAPI doesn't give support.
        if (element.getSourceType() == null
            || element.getSourceType().equals(PrimitiveType.PrimitiveTypeName.INT96)
            || element.getSourceType().equals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
            return new CustomWrapperSchemaParameter(element.getName(), type,
                (params.length == 0) ? null : params,
                !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                !isUpdateable, element.isNullable(), !isMandatory);
        }else {
            return new CustomWrapperSchemaParameter(element.getName(), type,
                (params.length == 0) ? null : params,
                isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                !isUpdateable, element.isNullable(), !isMandatory);
        }

    }
    
    
}
