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

import static com.denodo.connect.dfs.util.io.ConditionUtils.removeConditions;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;

public class ConditionUtilsTest {

    @Test
    public void buildFilter() {
    }

    @Test
    public void removeConditionsShouldBeNullTest() {

        final CustomWrapperCondition input = new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 200)});

        final CustomWrapperConditionHolder actual = removeConditions(new CustomWrapperConditionHolder(input),
            Arrays.asList("name", "response_code"));

        Assert.assertNull(actual.getComplexCondition());
    }


    @Test
    public void removeConditionsShouldBeTheSameTest() {

        final CustomWrapperCondition input = new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 200)});

        final CustomWrapperConditionHolder actual = removeConditions(new CustomWrapperConditionHolder(input),
            Arrays.asList("name", "address"));

        Assert.assertSame(input, actual.getComplexCondition());
    }

    @Test
    public void removeConditionsShouldBeTheSame_WithNoConditionsToRemoveTest() {

        final CustomWrapperCondition input = new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 200)});

        final CustomWrapperConditionHolder actual = removeConditions(new CustomWrapperConditionHolder(input),
            Collections.emptyList());

        Assert.assertSame(input, actual.getComplexCondition());
    }

    @Test
    public void removeAndConditionsTest() {

        final CustomWrapperCondition remainingCondition = new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
            CustomWrapperCondition.OPERATOR_GT,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)});

        final List<CustomWrapperCondition> childConditions = new ArrayList<>();
        childConditions.add(remainingCondition);
        childConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 200)}));
        final CustomWrapperCondition input = new CustomWrapperAndCondition(childConditions);

        final CustomWrapperConditionHolder actual = removeConditions(new CustomWrapperConditionHolder(input),
            Arrays.asList("name", "response_code"));

        Assert.assertEquals(remainingCondition.toString(), actual.getComplexCondition().toString());
    }

    @Test
    public void removeOrConditionsTest() {

        final CustomWrapperCondition remainingCondition = new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
            CustomWrapperCondition.OPERATOR_GT,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)});

        final LinkedList<CustomWrapperCondition> childConditions = new LinkedList<>();
        childConditions.add(remainingCondition);
        childConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 200)}));
        final CustomWrapperCondition input = new CustomWrapperOrCondition(childConditions);

        final CustomWrapperConditionHolder actual = removeConditions(new CustomWrapperConditionHolder(input),
            Arrays.asList("name", "response_code"));

        Assert.assertEquals(remainingCondition.toString(), actual.getComplexCondition().toString());
    }

    @Test
    public void removeComplexConditionsTest() {

        final LinkedList<CustomWrapperCondition> childOrConditions = new LinkedList<>();
        childOrConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
            CustomWrapperCondition.OPERATOR_GT,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)}));
        childOrConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 200)}));

        final CustomWrapperCondition orCondition = new CustomWrapperOrCondition(childOrConditions);

        final List<CustomWrapperCondition> childAndConditions = new ArrayList<>(2);
        childAndConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "request_port"),
            CustomWrapperCondition.OPERATOR_NE,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 9090)}));
        childAndConditions.add(orCondition);
        final CustomWrapperCondition input = new CustomWrapperAndCondition(childAndConditions);

        final List<CustomWrapperCondition> expectedChildConditions = new ArrayList<>(2);
        expectedChildConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "request_port"),
            CustomWrapperCondition.OPERATOR_NE,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 9090)}));
        expectedChildConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
            CustomWrapperCondition.OPERATOR_GT,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)}));
        final CustomWrapperCondition expected = new CustomWrapperAndCondition(expectedChildConditions);

        final CustomWrapperConditionHolder actual = removeConditions(new CustomWrapperConditionHolder(input),
            Arrays.asList("name", "response_code"));

        Assert.assertEquals(expected.toString(), actual.getComplexCondition().toString());
    }

    @Test
    public void removeComplexConditionsShouldBeNullTest() {

        final LinkedList<CustomWrapperCondition> childOrConditions = new LinkedList<>();
        childOrConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "sent_bytes"),
            CustomWrapperCondition.OPERATOR_GT,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 1000)}));
        childOrConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "response_code"),
            CustomWrapperCondition.OPERATOR_EQ,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 200)}));

        final CustomWrapperCondition orCondition = new CustomWrapperOrCondition(childOrConditions);

        final List<CustomWrapperCondition> childAndConditions = new ArrayList<>(2);
        childAndConditions.add(new CustomWrapperSimpleCondition(
            new CustomWrapperSimpleExpression(Types.VARCHAR, "request_port"),
            CustomWrapperCondition.OPERATOR_NE,
            new CustomWrapperExpression[] {new CustomWrapperSimpleExpression(Types.INTEGER, 9090)}));
        childAndConditions.add(orCondition);
        final CustomWrapperCondition input = new CustomWrapperAndCondition(childAndConditions);

        final CustomWrapperConditionHolder actual = removeConditions(new CustomWrapperConditionHolder(input),
            Arrays.asList("name", "response_code", "request_port", "sent_bytes"));

        Assert.assertNull(actual.getComplexCondition());
    }

}