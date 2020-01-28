package com.denodo.connect.hadoop.hdfs.util.schema;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.type.AvroTypeUtils;


public final class AvroSchemaUtils {

    private AvroSchemaUtils() {

    }

    public static Schema buildSchema(final Map<String, String> inputValues, final Configuration conf) throws IOException {

        // The two input parameters AVSC_FILE_PATH and AVSC_JSON are mutually exclusive.
        final String schemaFilePath = inputValues.get(Parameter.AVRO_SCHEMA_PATH);
        if (StringUtils.isNotBlank(schemaFilePath)) {
            return AvroSchemaUtils.buildSchema(conf, schemaFilePath);
        }

        final String schema = inputValues.get(Parameter.AVRO_SCHEMA_JSON);
        if (StringUtils.isNotBlank(schema)) {
            return AvroSchemaUtils.buildSchema(schema);
        }

        throw new IllegalArgumentException("One of these parameters: '"
            + Parameter.AVRO_SCHEMA_PATH + "' or '" + Parameter.AVRO_SCHEMA_JSON + "' must be specified");

    }

    private static Schema buildSchema(final Configuration conf, final String schemaFilePath) throws IOException {

        FSDataInputStream dataInputStream = null;
        try {

            final FileSystem fileSystem = FileSystem.get(conf);
            final Path avscPath = new Path(schemaFilePath);
            dataInputStream = fileSystem.open(avscPath);

            return  new Schema.Parser().parse(dataInputStream);

        } finally {
            IOUtils.closeQuietly(dataInputStream);
        }

    }

    private static Schema buildSchema(final String schema) {
        return new Schema.Parser().parse(schema);
    }

   /**
    *
    * A schema may be one of:
    *
    * <ul>
    * <li>A record, mapping field names to field value data</li>
    * <li>An enum, containing one of a small set of symbols</li>
    * <li>An array of values, all of the same schema</li>
    * <li>A map, containing string/value pairs, of a declared schema</li>
    * <li>A union of other schemas</li>
    * <li>A fixed sized binary object</li>
    * <li>A unicode string</li>
    * <li>A sequence of bytes</li>
    * <li>A 32-bit signed int</li>
    * <li>A 64-bit signed long</li>
    * <li>A 32-bit IEEE single-float</li>
    * <li>A 64-bit IEEE double-float</li>
    * <li>A boolean</li>
    * <li>Null</li>
    * </ul>
    *
    * <p>
    * See the Avro documentation for more information
    * (<a href="http://avro.apache.org/docs/1.5.4/api/java/org/apache/avro/Schema.html">here</a>)
    * </p>
    */
    public static SchemaElement buildSchema(final Schema schema, final String schemaName) {

        final Type schemaType = schema.getType();
        final Class<?> javaType = AvroTypeUtils.toJava(schema);
        if (AvroTypeUtils.isSimple(schemaType) || AvroTypeUtils.isEnum(schemaType) || AvroTypeUtils.isFixed(schemaType)) {
            return new SchemaElement(schemaName, javaType);

        } else if (AvroTypeUtils.isArray(schemaType)) {
            final Schema arrayElement = schema.getElementType();
            final SchemaElement schemaElement = new SchemaElement(schemaName, javaType);
            schemaElement.add(buildSchema(arrayElement, arrayElement.getName()));

            return schemaElement;
        } else if (AvroTypeUtils.isUnion(schemaType)) {
            // Only support UNION types with two types, being one of them NULL
            final List<Schema> schemas = schema.getTypes();
            final Schema notNullSchema = AvroTypeUtils.getNotNull(schemas);
            return buildSchema(notNullSchema, schemaName);
        } else if (AvroTypeUtils.isRecord(schemaType)) {
            final String recordName = schema.getName();
            final SchemaElement schemaElement = new SchemaElement(recordName, javaType);
            for (final Field f : schema.getFields()) {
                schemaElement.add(buildSchema(f.schema(), f.name()));
            }
            return schemaElement;

        } else if (AvroTypeUtils.isMap(schemaType)) {
            // map is a struct with two fields: key and value
            final SchemaElement schemaElement = new SchemaElement(schemaName, javaType);

            schemaElement.add(new SchemaElement(Parameter.KEY, String.class));
            schemaElement.add(buildSchema(schema.getValueType(), Parameter.VALUE));

            return schemaElement;

        }

        throw new IllegalArgumentException("Unsupported type: " + schemaType.name());
    }

}
