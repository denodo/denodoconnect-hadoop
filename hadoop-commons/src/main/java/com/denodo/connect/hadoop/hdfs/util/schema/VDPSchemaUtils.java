package com.denodo.connect.hadoop.hdfs.util.schema;

import java.util.Collection;
import java.util.List;

import org.apache.parquet.schema.PrimitiveType;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.type.TypeUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;


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

    public static boolean isProjected(final String field, final List<CustomWrapperFieldExpression> projectedFields) {

        for (final CustomWrapperFieldExpression projectedField : projectedFields) {
            if (field.equals(projectedField.getName())) {
                return true;
            }
        }

        return false;
    }

    public static boolean isSchemaField(final String fields, final CustomWrapperSchemaParameter[] schemaParameters) {

        final String[] fragmentsFields = fields.split(",");
        boolean found = false;
        for (final String field : fragmentsFields) {
            for (int i = 0; i < schemaParameters.length && !found; i++) {
                final CustomWrapperSchemaParameter parameter = schemaParameters[i];
                if (field.trim().equals(parameter.getName())) {
                    found = true;
                }
            }
            if (!found) {
                return false;
            }
            found = false;
        }

        return true;
    }

    public static String toString(final CustomWrapperSchemaParameter[] schemaParameters) {

        final StringBuilder sb = new StringBuilder();
        final int lastParameter = schemaParameters.length - 1;
        int i = 0;
        for(final CustomWrapperSchemaParameter parameter : schemaParameters) {
            sb.append(parameter.getName());
            if (i != lastParameter) {
                sb.append(", ");
            }

            i++;
        }

        return sb.toString();
    }
}
