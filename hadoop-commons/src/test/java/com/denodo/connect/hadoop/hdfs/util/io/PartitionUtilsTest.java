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

import static com.denodo.connect.hadoop.hdfs.util.io.PartitionUtils.getPartitionFields;
import static com.denodo.connect.hadoop.hdfs.util.io.PartitionUtils.getPartitionValues;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;

public class PartitionUtilsTest {

    @Test
    public void getPartitionFieldsTest() {

        final Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=404/elb_logs_002__25630654204875.dat");

        final List<Type> expectedTypes = new ArrayList<>(2);
        expectedTypes.add(Types.optional(BINARY).as(OriginalType.UTF8).named("name"));
        expectedTypes.add(Types.optional(INT32).named("response_code"));

        Assert.assertEquals(expectedTypes, getPartitionFields(path));
    }

    @Test
    public void partitionFieldsShouldBeEmptyTest() {

        Path path = new Path("s3a://denodo/elb_partition/name/response_code/elb_logs_002__25630654204875.dat");
        Assert.assertEquals(Collections.emptyList(), getPartitionFields(path));

        path = new Path("s3a://denodo/elb_partition/name/response_code=/elb_logs_002__25630654204875.dat");
        Assert.assertEquals(Collections.emptyList(), getPartitionFields(path));
    }

    @Test
    public void getPartitionValuesTest() {

        final Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=404/elb_logs_002__25630654204875.dat");

        final List<Comparable> expectedValues = new ArrayList<>(2);
        expectedValues.add("elb_demo_002");
        expectedValues.add(404);

        Assert.assertEquals(expectedValues, getPartitionValues(path));
    }

    @Test
    public void partitionValuesShouldBeEmptyTest() {

        Path path = new Path("s3a://denodo/elb_partition/name/response_code/elb_logs_002__25630654204875.dat");
        Assert.assertEquals(Collections.emptyList(), getPartitionValues(path));

        path = new Path("s3a://denodo/elb_partition/name/response_code=/elb_logs_002__25630654204875.dat");
        Assert.assertEquals(Collections.emptyList(), getPartitionValues(path));
    }
}