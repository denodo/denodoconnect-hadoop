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
package com.denodo.connect.hadoop.hdfs.util.schema;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.type.ParquetTypeUtils;

public class ParquetSchemaUtils {

    /**
     * This is the list of names that can have the repeated element list
     */
    private static final List<String> LIST_VALUES = Collections.singletonList("list");
    
    /**
     * This is the list of names that can have the repeated element map 
     */
    private static final List<String> MAP_VALUES = Arrays.asList("map", "key_value");

    /**
     * This method build the parquet schema
     */
    public static  SchemaElement buildSchema(final GroupType group, final SchemaElement schemaElement) {

        for (final Type field : group.getFields()) {
            try {
                final boolean isNullable = field.getRepetition() != Repetition.REQUIRED;
                if (field.isPrimitive()) {
                    addPrimitiveType(schemaElement, field, isNullable);
                } else if (ParquetTypeUtils.isGroup(field)) {
                    addGroupType(schemaElement, field, isNullable);
                } else if (ParquetTypeUtils.isList(field)) {
                    addListType(schemaElement, field, isNullable);
                } else if (ParquetTypeUtils.isMap(field)) {
                    addMapType(schemaElement, field, isNullable);
                }
            } catch (final ClassCastException e) {
                throw new IllegalArgumentException("Unsupported type: " + field);
            }
        }
        return schemaElement;
    }

    /**
     * Method to add the map types to the schema
     */
    private static void addMapType(final SchemaElement schemaElement, final Type field, final boolean isNullable) {
        try {
            // This element have as OriginalType MAP. The standard MAPS in parquet should have repeated as next element
            final Type nextFieldMap = field.asGroupType().getFields() != null && !field.asGroupType().getFields()
                .isEmpty() ? field.asGroupType().getFields().get(0) : null;
            
            if (nextFieldMap != null
                    && MAP_VALUES.contains(nextFieldMap.getName()) 
                    && nextFieldMap.getRepetition() != null
                    && nextFieldMap.getRepetition() == Repetition.REPEATED
                    && nextFieldMap.asGroupType().getFields() != null
                    && nextFieldMap.asGroupType().getFieldCount() == 2
                    && nextFieldMap.asGroupType().getFields().get(0).getRepetition() == Repetition.REQUIRED) {

                //Create the map schema 
                final SchemaElement schemaElementMap = new SchemaElement(field.getName(), Map.class, isNullable);
                
                //Take the key field 
                final Type nextFieldKey = nextFieldMap.asGroupType().getFields().get(0);
                //Take the value field and if is nullable information
                final Type nextFieldValue = nextFieldMap.asGroupType().getFields().get(1);
                final boolean nextFieldIsNullable = nextFieldValue.getRepetition() != Repetition.REQUIRED;
                
                //Add the key to the schema
                if (nextFieldKey.isPrimitive()) {
                    addPrimitiveType(schemaElementMap, nextFieldKey, nextFieldIsNullable);
                } else {
                    throw new IllegalArgumentException("ERROR when building the schema. The list element " + field.getName()
                        + " doesn't have a valid format. Key should be a primitive type");
                }
                //Add the value to the schema
                if (nextFieldValue.isPrimitive()) {
                    addPrimitiveType(schemaElementMap, nextFieldValue, nextFieldIsNullable);
                } else {
                    schemaElementMap.add(buildSchema(nextFieldValue.asGroupType(), schemaElementMap));
                }
                schemaElement.add(schemaElementMap);
            } else {
                throw new IllegalArgumentException("ERROR when building the schema. The list element " + field.getName() + " doesn't have a valid format");
            }
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("ERROR when building the schema for type: " + field);
        }
    }

    /**
     * Method to add the list types to the schema
     */
    private static void addListType(final SchemaElement schemaElement, final Type field, final boolean isNullable) {
        try {
            //This element have as OriginalType LIST. The standard LISTS in parquet should have repeated as next element
            final Type nextFieldList = field.asGroupType().getFields() != null && !field.asGroupType().getFields()
                .isEmpty() ? field.asGroupType().getFields().get(0) : null;
            
            if (nextFieldList != null && LIST_VALUES.contains(nextFieldList.getName()) && nextFieldList.getRepetition() != null
                    && nextFieldList.getRepetition() == Repetition.REPEATED && nextFieldList.asGroupType().getFields() != null) {
                
                final SchemaElement schemaElementList = new SchemaElement(field.getName(), List.class, isNullable);
                schemaElement.add(buildSchema(nextFieldList.asGroupType(), schemaElementList));
            } else {
                //For Backward-compatibility is necessary to add code here. Two list elements is not necessary. 
                throw new IllegalArgumentException("ERROR when building the schema. The list element " + field.getName() + " doesn't have a valid format");
            }
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("ERROR when building the schema for type: " + field);
        }
    }

    /**
     * Method to add the group types to the schema
     */
    private static void addGroupType(final SchemaElement schemaElement, final Type field, final boolean isNullable) {
        try {
            final SchemaElement schemaElementGroup = new SchemaElement(field.getName(), Object.class, isNullable);
            schemaElement.add(buildSchema(field.asGroupType(), schemaElementGroup));
        }  catch (final ClassCastException e) {
            throw new IllegalArgumentException("ERROR when building the schema for type: " + field);
        }
    }

    /**
     * Method to add the primitive types to the schema
     */
    private static void addPrimitiveType(final SchemaElement schemaElement, final Type field, final boolean isNullable) {
        schemaElement.add(new SchemaElement(field.getName(), ParquetTypeUtils.toJava(field), field.asPrimitiveType().getPrimitiveTypeName(), isNullable));
    }


    public static List<BlockMetaData> getRowGroups(final Configuration configuration, final Path filePath) throws IOException {
        List<BlockMetaData> rowGroups  = null;
        try (final ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, configuration))) {

            rowGroups = parquetFileReader.getRowGroups();
        }
        return rowGroups;
    }

}
