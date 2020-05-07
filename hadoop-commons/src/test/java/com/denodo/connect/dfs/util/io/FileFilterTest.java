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
package com.denodo.connect.dfs.util.io;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;

public class FileFilterTest {

    @Test
    //  VDP condition: WHERE sent_bytes > 1000 OR response_code = 500
    public void orPartitionConditionTest() {

        final FileFilter filter = new FileFilter(null);

        final LinkedList<CustomWrapperCondition> orConditions = new LinkedList<>();
        orConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
            CustomWrapperCondition.OPERATOR_GT,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)}));

        orConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 500)}));


        filter.addPartitionConditions(Collections.singletonList("response_code"),
            Arrays.asList("sent_bytes", "response_code"),
            new CustomWrapperOrCondition(orConditions));


        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        // should be accepted despite response_code value in path, because is an OR condition
        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

    }

    @Test
    //  VDP condition: WHERE name = 'demo_001' OR response_code < 500
    public void orPartitionConditionsTest() {

        final FileFilter filter = new FileFilter(null);

        final LinkedList<CustomWrapperCondition> orConditions = new LinkedList<>();
        orConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "name"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.VARCHAR, "elb_demo_001")}));
        orConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_LT,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 500)}));


        filter.addPartitionConditions(Arrays.asList("name", "response_code"),
            Arrays.asList("name", "response_code"),
            new CustomWrapperOrCondition(orConditions));

        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertFalse(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_001/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

    }


    @Test
    //  VDP condition: WHERE sent_bytes > 1000 AND response_code = 500
    public void andPartitionConditionTest() {

        final FileFilter filter = new FileFilter(null);


        filter.addPartitionConditions(Collections.singletonList("response_code"),
            Arrays.asList("sent_bytes", "response_code"),
            new CustomWrapperAndCondition(Arrays.asList(
                new CustomWrapperSimpleCondition(
                    new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
                    CustomWrapperCondition.OPERATOR_GT,
                    new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)}),
                new CustomWrapperSimpleCondition(
                    new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
                    CustomWrapperCondition.OPERATOR_EQ,
                    new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 500)}))));

        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertFalse(filter.accept(path));

    }

    @Test
    //  VDP condition: WHERE name = 'demo_001' AND response_code < 500
    public void andPartitionConditionsTest() {

        final FileFilter filter = new FileFilter(null);;


        filter.addPartitionConditions(Arrays.asList("response_code", "name"),
            Arrays.asList("name", "response_code"),
            new CustomWrapperAndCondition(Arrays.asList(
                new CustomWrapperSimpleCondition(
                    new CustomWrapperSimpleExpression(Types.VARCHAR, "name"),
                    CustomWrapperCondition.OPERATOR_EQ,
                    new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.VARCHAR, "elb_demo_001")}),
                new CustomWrapperSimpleCondition(
                    new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
                    CustomWrapperCondition.OPERATOR_LT,
                    new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 500)}))));

        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertFalse(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertFalse(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_001/response_code=404/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

    }

    @Test
    //  VDP condition: WHERE response_code = 500
    public void simplePartitionConditionAsIntegerTest() {

        final FileFilter filter = new FileFilter(null);

        filter.addPartitionConditions(Collections.singletonList("response_code"),
            Collections.singletonList("response_code"),
            new CustomWrapperSimpleCondition(
                new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
                CustomWrapperCondition.OPERATOR_EQ,
                new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 500)}));

        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertFalse(filter.accept(path));

    }

    @Test
    //  VDP condition: WHERE response_code = '500'
    public void simplePartitionConditionAsStringTest() {

        final FileFilter filter = new FileFilter(null);

        filter.addPartitionConditions(Collections.singletonList("response_code"),
            Collections.singletonList("response_code"),
            new CustomWrapperSimpleCondition(
                new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
                CustomWrapperCondition.OPERATOR_EQ,
                new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.VARCHAR, "500")}));

        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertFalse(filter.accept(path));

    }

    @Test
    //  VDP condition: WHERE sent_bytes > 1000
    public void noPartitionConditionTest() {

        final FileFilter filter = new FileFilter(null);

        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        // should be accepted despite response_code value in path, because is an OR condition
        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

    }

    @Test
    //  VDP condition: WHERE name = "elb_demo_002" AND (sent_bytes > 1000 OR response_code = 500)
    public void andOrPartitionConditionTest() {

        final FileFilter filter = new FileFilter(null);

        final LinkedList<CustomWrapperCondition> orConditions = new LinkedList<>();
        orConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
            CustomWrapperCondition.OPERATOR_GT,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)}));

        orConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.BIGINT, 500L)}));

        final CustomWrapperCondition andCondition = new CustomWrapperAndCondition(Arrays.asList(
            new CustomWrapperSimpleCondition(
                new CustomWrapperSimpleExpression(Types.VARCHAR, "name"),
                CustomWrapperCondition.OPERATOR_EQ,
                new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.VARCHAR, "elb_demo_002")}),
            new CustomWrapperOrCondition(orConditions)));

        filter.addPartitionConditions(Arrays.asList("response_code", "name"),
            Arrays.asList("sent_bytes", "response_code", "name"),
            andCondition);


        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_001/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertFalse(filter.accept(path));

    }

    @Test
    //  VDP condition: WHERE name = "elb_demo_002" OR (sent_bytes > 1000 AND response_code = 500)
    public void orAndPartitionConditionTest() {

        final FileFilter filter = new FileFilter(null);

        final LinkedList<CustomWrapperCondition> orConditions = new LinkedList<>();
        orConditions.add(new CustomWrapperSimpleCondition(
                new CustomWrapperSimpleExpression(Types.VARCHAR, "name"),
                CustomWrapperCondition.OPERATOR_EQ,
                new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.VARCHAR, "elb_demo_002")}));
        orConditions.add(new CustomWrapperAndCondition(Arrays.asList(
            new CustomWrapperSimpleCondition(
                new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
                CustomWrapperCondition.OPERATOR_GT,
                new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)}),
            new CustomWrapperSimpleCondition(
                new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
                CustomWrapperCondition.OPERATOR_EQ,
                new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.BIGINT, 500L)}))));

        filter.addPartitionConditions(Arrays.asList("name", "response_code"),
            Arrays.asList("name", "sent_bytes", "response_code"),
            new CustomWrapperOrCondition(orConditions));

        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_001/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertFalse(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_001/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));
    }


    @Test
    //  VDP condition: WHERE sent_bytes > 1000 AND (name = "elb_demo_002" OR response_code = 500)
    public void andOrPartitionCondition2Test() {

        final FileFilter filter = new FileFilter(null);

        final LinkedList<CustomWrapperCondition> orConditions = new LinkedList<>();
        orConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "name"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.VARCHAR, "elb_demo_002")}));
        orConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.BIGINT, 500L)}));

        final CustomWrapperCondition andCondition = new CustomWrapperAndCondition(Arrays.asList(
            new CustomWrapperSimpleCondition(
                new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
                CustomWrapperCondition.OPERATOR_GT,
                new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)}),
            new CustomWrapperOrCondition(orConditions)));

        filter.addPartitionConditions(Arrays.asList("response_code", "name"),
            Arrays.asList("sent_bytes", "response_code", "name"),
            andCondition);


        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_001/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_001/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertFalse(filter.accept(path));

    }

    @Test
    //  VDP condition: WHERE sent_bytes > 1000 OR (name = "elb_demo_002" AND response_code = 500)
    public void orAndPartitionCondition2Test() {

        final FileFilter filter = new FileFilter(null);

        final LinkedList<CustomWrapperCondition> orConditions = new LinkedList<>();
        orConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
            CustomWrapperCondition.OPERATOR_GT,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)}));
        orConditions.add(new CustomWrapperAndCondition(Arrays.asList(
            new CustomWrapperSimpleCondition(
                new CustomWrapperSimpleExpression(Types.VARCHAR, "name"),
                CustomWrapperCondition.OPERATOR_EQ,
                new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.VARCHAR, "elb_demo_002")}),
            new CustomWrapperSimpleCondition(
                new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
                CustomWrapperCondition.OPERATOR_EQ,
                new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.BIGINT, 500L)}))));

        filter.addPartitionConditions(Arrays.asList("name", "response_code"),
            Arrays.asList("name", "sent_bytes", "response_code"),
            new CustomWrapperOrCondition(orConditions));

        Path path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_002/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_001/response_code=200/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));

        path = new Path("s3a://denodo/elb_partition/name=elb_demo_001/response_code=500/elb_logs_002__25630654204875.dat");
        Assert.assertTrue(filter.accept(path));
    }
}