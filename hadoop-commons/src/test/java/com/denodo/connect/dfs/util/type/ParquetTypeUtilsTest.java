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
package com.denodo.connect.dfs.util.type;

import static com.denodo.connect.dfs.util.type.ParquetTypeUtils.inferParquetType;
import static com.denodo.connect.dfs.util.type.ParquetTypeUtils.inferParquetValue;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.Assert;

public class ParquetTypeUtilsTest {

    @org.junit.Test
    public void inferParquetTypeTest() {

        Assert.assertEquals(Types.optional(INT32).named("rate"), inferParquetType("rate", "102"));
        Assert.assertEquals(Types.optional(INT64).named("sentBytes"), inferParquetType("sentBytes", "21474830000"));
        Assert.assertEquals(Types.optional(FLOAT).named("tax"), inferParquetType("tax", "3.14"));
        Assert.assertEquals(Types.optional(BINARY).as(stringType()).named("demoId"), inferParquetType("demoId", "demo_003"));
        Assert.assertEquals(Types.optional(INT32).as(dateType()).named("lastUpdate"), inferParquetType("lastUpdate", "2014-1-17"));
        Assert.assertEquals(Types.optional(INT32).as(timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("arrivalTime"),
            inferParquetType("arrivalTime", "14:32:17"));
    }

    @org.junit.Test
    public void inferParquetValueTest() {

        Assert.assertEquals(102, inferParquetValue("102"));
        Assert.assertEquals(21474830000L, inferParquetValue("21474830000"));
        Assert.assertEquals(3.14f, inferParquetValue("3.14"));
        Assert.assertEquals("demo_003", inferParquetValue("demo_003"));

    }
}