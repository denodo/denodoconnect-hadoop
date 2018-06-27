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


import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.type.ParquetTypeUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;

public class ParquetSchemaUtils {

    private static final String REPEATED_VALUE = "REPEATED";
    private static final String LIST_VALUE = "list";

    public static  SchemaElement buildSchema(GroupType group, SchemaElement schemaElement) throws CustomWrapperException {

        for(Type field:   group.getFields()){
            try {
                if (field.isPrimitive()) {
                    
                    schemaElement.add(new SchemaElement(field.getName(), ParquetTypeUtils.toJava(field)));

                } else if (field.asGroupType().getFields().size() > 0 && field.getOriginalType() == null) {
                    
                    SchemaElement schemaElementGroup = new SchemaElement(field.getName(), Object.class);
                    schemaElement.add(buildSchema(field.asGroupType(), schemaElementGroup));
                    
                } else if (field.asGroupType().getFields().size() > 0 && field.getOriginalType() != null
                        && field.getOriginalType().equals(OriginalType.LIST)) {

                    //This element have as OriginalType LIST. The standard LISTS in parquet should have repeated as next element
                    Type nextFieldList = field.asGroupType().getFields() != null ? field.asGroupType().getFields().get(0) : null;
                    
                    if (nextFieldList != null && StringUtils.equals(nextFieldList.getName(), LIST_VALUE) && nextFieldList.getRepetition() != null
                            && StringUtils.equals(nextFieldList.getRepetition().name(), REPEATED_VALUE) && nextFieldList.asGroupType().getFields() != null) {

                        // Type nextFieldelement = nextFieldList.asGroupType().getFields() != null ? nextFieldList.asGroupType().getFields().get(0) : null;
                        SchemaElement schemaElementList = new SchemaElement(field.getName(), List.class);
                        schemaElement.add(buildSchema(nextFieldList.asGroupType(), schemaElementList));
                    } else {
                        //FIXME: For Backward-compatibility add else. Two list elements is not necessary
                        throw new CustomWrapperException("ERROR When try to buildSchema. The list element " + field.getName() + " don't have a valid format");
                    }

                }
            } catch (ClassCastException e) {
                throw new CustomWrapperException("ERROR When try to convert to GroupType", e);
            }
        }
        return schemaElement;
    }

}
