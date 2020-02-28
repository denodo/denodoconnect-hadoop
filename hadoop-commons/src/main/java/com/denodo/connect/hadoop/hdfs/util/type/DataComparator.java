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
package com.denodo.connect.hadoop.hdfs.util.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;


/**
 * Adapted from class DataComparator from module dt-denodotesting.
 */
public class DataComparator {

    public static int compare(final Object obj1, final Object obj2) {

        if (isBlank(obj1) && isBlank(obj2)) {
            return 0;
        }
        if (isBlank(obj1)) {
            return -1;
        }
        if (isBlank(obj2)) {
            return 1;
        }

        // First, we turn primitives into objects
        Object normalizedObj1 = normalizePrimitives(obj1);
        Object normalizedObj2 = normalizePrimitives(obj2);

        // Now we make sure any TextualData (data represented as text which type is not known and might need to be
        // parsed as number/boolean) is normalized.
        normalizedObj1 = normalizeTextualData(normalizedObj1);
        normalizedObj2 = normalizeTextualData(normalizedObj2);

        // After normalizing TextualData, objects might have become null!
        if (normalizedObj1 == null && normalizedObj2 == null) {
            return 0;
        }
        if (normalizedObj1 == null) {
            return -1;
        }
        if (normalizedObj2 == null) {
            return 1;
        }


        // Compute the classes of both objects
        Class<?> normalizedObj1Class = normalizedObj1.getClass();
        Class<?> normalizedObj2Class = normalizedObj2.getClass();

        // We normalized the objects so that numeric values are compared disregarding their classes (e.g. long vs. int)
        normalizedObj1 = normalizeNumbers(normalizedObj1, normalizedObj1Class, normalizedObj2Class);
        normalizedObj2 = normalizeNumbers(normalizedObj2, normalizedObj1Class, normalizedObj2Class);

        // We obtain the completely-normalized classes being compared
        normalizedObj1Class = normalizedObj1.getClass();
        normalizedObj2Class = normalizedObj2.getClass();


        /*
         * First comparison attempt, including all normalizations (textual data + numeric)
         */
        if (normalizedObj1Class.isAssignableFrom(normalizedObj2Class) || normalizedObj2Class.isAssignableFrom(normalizedObj1Class)) {

            if (Comparable.class.isAssignableFrom(normalizedObj1Class) && Comparable.class.isAssignableFrom(normalizedObj2Class)) {

                if (normalizedObj1Class.isAssignableFrom(normalizedObj2Class)) {
                    return ((Comparable) normalizedObj1).compareTo(normalizedObj2);
                }

                return -1 * ((Comparable) normalizedObj2).compareTo(normalizedObj1);

            }

            if (isClassBinaryData(normalizedObj1Class) && isClassBinaryData(normalizedObj2Class)) {
                return Arrays.equals((byte[]) normalizedObj1, (byte[])normalizedObj2) ? 0 : -1;
            }

        }


        // Object are not comparable, so we should be outputting a non-zero result in a consistent manner (using class names comparison)
        return normalizedObj1Class.getName().compareTo(normalizedObj2Class.getName());

    }

    private static boolean isBlank(final Object obj) {
        return obj == null || StringUtils.EMPTY.equals(obj.toString());
    }


    private static Object normalizePrimitives(final Object obj) {

        final Class<?> objClass = obj.getClass();

        if (!objClass.isPrimitive()) {
            return obj;
        }

        if (boolean.class.equals(objClass)) {
            final Boolean boxed = (Boolean) obj;
            return Boolean.valueOf(boxed);
        }

        if (short.class.equals(objClass)) {
            final Short boxed = (Short) obj;
            return Short.valueOf(boxed);
        }

        if (int.class.equals(objClass)) {
            final Integer boxed = (Integer) obj;
            return Integer.valueOf(boxed);
        }

        if (long.class.equals(objClass)) {
            final Long boxed = (Long) obj;
            return Long.valueOf(boxed);
        }

        if (float.class.equals(objClass)) {
            final Float boxed = (Float) obj;
            return Float.valueOf(boxed);
        }

        if (double.class.equals(objClass)) {
            final Double boxed = (Double) obj;
            return Double.valueOf(boxed);
        }

        if (char.class.equals(objClass)) {
            final Character boxed = (Character) obj;
            return Character.valueOf(boxed);
        }

        throw new IllegalStateException(
            "Unrecognized primitive type during normalization: " + objClass.getName());

    }

    private static Object normalizeTextualData(final Object obj) {

        if (!String.class.isAssignableFrom(obj.getClass())) {
            return obj;
        }

        final String value = (String) obj;

        /*
         *  First attempt at auto-conversion of object: let's see if it would be a valid number
         */
        if (areAllCharactersNumeric(value)) {

            if (areAllCharactersIntegerNumeric(value)) {
                try {
                    // It's an integer number. Default to BigInteger.
                    return new BigInteger(value);
                } catch (final NumberFormatException ignored) {
                    // nothing to do, it simply not a Number
                }
            }
            try {
                // This is a decimal number. Default to BigDecimal. Note there is no support for locale-dependent decimal comma.
                return new BigDecimal(value);
            } catch (final NumberFormatException ignored) {
                // nothing to do, it simply not a Number
            }
        }

        /*
         * Try to parse as boolean. Note we cannot directly use Boolean.valueOf because it will return Boolean.FALSE
         * for everything except the "true" string.
         */
        if ("true".equals(value) || "false".equals(value)) {
            return Boolean.valueOf(value);
        }

        /*
         * We don't really know what else to do with this piece of data, so we just return it as String.
         */
        return value;

    }


    private static boolean areAllCharactersNumeric(final String text) {

        char c;
        int n = text.length();
        if (n == 0) {
            return false;
        }

        int i = 0;
        while (n-- != 0) {
            c = text.charAt(i);
            if ((c < '0' || c > '9') && c != '.' && !(i == 0 && c == '-')) {
                return false;
            }
            i++;
        }
        return true;
    }

    private static boolean areAllCharactersIntegerNumeric(final String text) {

        char c;
        int n = text.length();
        if (n == 0) {
            return false;
        }

        int i = 0;
        while (n-- != 0) {
            c = text.charAt(i++);
            if ((c < '0' || c > '9') && !(i == 0 && c == '-')) {
                return false;
            }
        }
        return true;
    }

    private static Object normalizeNumbers(final Object obj, final Class<?> class1, final Class<?> class2) {

        if (class1.equals(class2)) {
            return obj;
        }

        if (!isClassNumeric(class1) || !isClassNumeric(class2)) {
            // They are not both numbers, so there's nothing we can do
            return obj;
        }

        /*
         * In order to be more memory-efficient, if BOTH classes are numeric integers (short, int, long, BigInteger)
         * we will normalize to the smallest class possible (the biggest of class1 and class2).
         *
         * In any other case, we will normalize to BigDecimal.
         */
        if (isClassNumericInteger(class1) && isClassNumericInteger(class2)) {

            if (BigInteger.class.equals(class1) || BigInteger.class.equals(class2)) {
                // Normalize to BigInteger
                if (BigInteger.class.equals(obj.getClass())) {
                    return obj;
                }
                if (Long.class.equals(obj.getClass())) {
                    return BigInteger.valueOf(((Long)obj).longValue());
                }
                if (Integer.class.equals(obj.getClass())) {
                    return BigInteger.valueOf(((Integer)obj).longValue());
                }
                if (Short.class.equals(obj.getClass())) {
                    return BigInteger.valueOf(((Short)obj).longValue());
                }
                throw new IllegalStateException(
                    "Class was classified as numeric but cannot be recognized: " + obj.getClass().getName());
            }

            if (Long.class.equals(class1) || Long.class.equals(class2)) {
                // Normalize to Long
                if (Long.class.equals(obj.getClass())) {
                    return obj;
                }
                if (Integer.class.equals(obj.getClass())) {
                    return Long.valueOf(((Integer)obj).longValue());
                }
                if (Short.class.equals(obj.getClass())) {
                    return Long.valueOf(((Short)obj).longValue());
                }
                throw new IllegalStateException(
                    "Class was classified as numeric but cannot be recognized: " + obj.getClass().getName());
            }

            if (Integer.class.equals(class1) || Integer.class.equals(class2)) {
                // Normalize to Integer
                if (Integer.class.equals(obj.getClass())) {
                    return obj;
                }
                if (Short.class.equals(obj.getClass())) {
                    return Integer.valueOf(((Short)obj).intValue());
                }
                throw new IllegalStateException(
                    "Class was classified as numeric but cannot be recognized: " + obj.getClass().getName());
            }

            if (Short.class.equals(class1) || Short.class.equals(class2)) {
                // Normalize to Short
                if (Short.class.equals(obj.getClass())) {
                    return obj;
                }
                throw new IllegalStateException(
                    "Class was classified as numeric but cannot be recognized: " + obj.getClass().getName());
            }

            throw new IllegalStateException(
                "Classes were classified as numeric but cannot be recognized: " + class1.getName() + " and " + class2.getName());

        }

        // At least one is a non-integer, so we will normalize everything to BigDecimal

        if (BigDecimal.class.equals(obj.getClass())) {
            return obj;
        }
        if (BigInteger.class.equals(obj.getClass())) {
            return new BigDecimal((BigInteger)obj);
        }
        if (Double.class.equals(obj.getClass())) {
            return BigDecimal.valueOf(((Double)obj).doubleValue());
        }
        if (Float.class.equals(obj.getClass())) {
            return BigDecimal.valueOf(((Float)obj).doubleValue());
        }
        if (Long.class.equals(obj.getClass())) {
            return BigDecimal.valueOf(((Long)obj).longValue());
        }
        if (Integer.class.equals(obj.getClass())) {
            return BigDecimal.valueOf(((Integer)obj).longValue());
        }
        if (Short.class.equals(obj.getClass())) {
            return BigDecimal.valueOf(((Short)obj).longValue());
        }
        throw new IllegalStateException(
            "Class was classified as numeric but cannot be recognized: " + obj.getClass().getName());

    }


    private static boolean isClassNumeric(final Class<?> clazz) {
        return isClassNumericInteger(clazz) || isClassNumericDecimal(clazz);
    }

    private static boolean isClassNumericInteger(final Class<?> clazz) {
        return Integer.class.isAssignableFrom(clazz) || Long.class.isAssignableFrom(clazz) ||
            BigInteger.class.isAssignableFrom(clazz) || Short.class.isAssignableFrom(clazz);
    }

    private static boolean isClassNumericDecimal(final Class<?> clazz) {
        return Float.class.isAssignableFrom(clazz) || Double.class.isAssignableFrom(clazz) ||
            BigDecimal.class.isAssignableFrom(clazz);
    }

    private static boolean isClassBinaryData(final Class<?> clazz) {
        return byte[].class.isAssignableFrom(clazz);
    }

}