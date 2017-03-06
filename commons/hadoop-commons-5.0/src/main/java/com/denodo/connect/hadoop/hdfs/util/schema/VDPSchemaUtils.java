package com.denodo.connect.hadoop.hdfs.util.schema;

import java.util.Collection;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.type.TypeUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;


public final class VDPSchemaUtils {

    private VDPSchemaUtils() {

    }

    public static CustomWrapperSchemaParameter[] buildSchema(Collection<SchemaElement> schema) {

        CustomWrapperSchemaParameter[] vdpSchema = new CustomWrapperSchemaParameter[schema.size()];
        int i = 0;
        for (SchemaElement element : schema) {
            vdpSchema[i++] = buildSchemaParameter(element);
        }

        return vdpSchema;

    }

    public static CustomWrapperSchemaParameter buildSchemaParameter(SchemaElement element) {

        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;


        CustomWrapperSchemaParameter[] params = new CustomWrapperSchemaParameter[element.getElements().size()];
        int i = 0;
        for (SchemaElement e : element.getElements()) {
            params[i++] = buildSchemaParameter(e);
        }

        int type = TypeUtils.toSQL(element.getType());
        return new CustomWrapperSchemaParameter(element.getName(), type,
            (params.length == 0) ? null : params,
            !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
            !isUpdateable, isNullable, !isMandatory);
    }

    public static CustomWrapperSchemaParameter[] buildSchemaParameter(Collection<SchemaElement> elements) {

        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;


        CustomWrapperSchemaParameter[] params = new CustomWrapperSchemaParameter[elements.size()];
        int i = 0;
        for (SchemaElement e : elements) {
            params[i++] = new CustomWrapperSchemaParameter(e.getName(),TypeUtils.toSQL(e.getType()),
                    (params.length == 0) ? null : params,
                            !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                            !isUpdateable, isNullable, !isMandatory);
        }

        
        return params;
    }

}
