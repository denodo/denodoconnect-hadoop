/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2020, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.hdfs.util.io;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import com.denodo.connect.hadoop.hdfs.util.type.ParquetTypeUtils;

public class PartitionUtils {

    private static final String NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    private PartitionUtils() {

    }

    public static List<Type> getPartitionFields(final Path path) {

        final List<Type> partitionFields = new ArrayList<>();

        final String pathAsString = path.toUri().getPath();
        final String[] pathFragments = pathAsString.split("/");
        if (pathFragments.length > 1) {
            for (int i = 0; i < pathFragments.length - 1; i++) {
                final String[] nameValuePair = pathFragments[i].split("=");
                if (nameValuePair.length == 2) {
                    final String rawValue = NULL_PARTITION.equals(nameValuePair[1]) ? null : nameValuePair[1];
                    final PrimitiveType primitiveType = ParquetTypeUtils.inferParquetType(nameValuePair[0], rawValue);
                    partitionFields.add(primitiveType);
                }
            }
        }

        return partitionFields;
    }

    public static List<Comparable> getPartitionValues(final Path path) {

        final List<Comparable> partitionValues = new ArrayList<>();

        final String pathAsString = path.toUri().getPath();
        final String[] pathFragments = pathAsString.split("/");
        if (pathFragments.length > 1) {
            for (int i = 0; i < pathFragments.length - 1; i++) {
                final String[] nameValuePair = pathFragments[i].split("=");
                if (nameValuePair.length == 2) {
                    final String rawValue = NULL_PARTITION.equals(nameValuePair[1]) ? null : nameValuePair[1];
                    final Comparable value = ParquetTypeUtils.inferParquetValue(rawValue);
                    partitionValues.add(value);
                }
            }
        }
        return partitionValues;
    }


}