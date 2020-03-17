/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.denodo.connect.hadoop.hdfs.wrapper.providers;

import static com.denodo.connect.hadoop.hdfs.wrapper.providers.Constants.AWS_CREDENTIALS_PROVIDER;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;

/**
 *  Required by AWSAssumedRoleCredentialProvider.
 *  From https://github.com/apache/hadoop/commit/9a013b255f301c557c3868dc1ad657202e9e7a67,
 *  as in hadoop-aws v2.x (the one compatible with java 7) does not exists.
 *
 *
 * Utility methods for S3A code.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class S3AUtils {

  private static final Logger LOG = LoggerFactory.getLogger(S3AUtils.class);
  static final String CONSTRUCTOR_EXCEPTION = "constructor exception";
  static final String INSTANTIATION_EXCEPTION
      = "instantiation exception";
  static final String NOT_AWS_PROVIDER =
      "does not implement AWSCredentialsProvider";
  static final String ABSTRACT_PROVIDER =
      "is abstract and therefore cannot be created";


  private S3AUtils() {
  }



  /**
   * Load list of AWS credential provider/credential provider factory classes.
   * @param conf configuration
   * @param key key
   * @param defaultValue list of default values
   * @return the list of classes, possibly empty
   * @throws IOException on a failure to load the list.
   */
  public static Class<?>[] loadAWSProviderClasses(Configuration conf,
      String key,
      Class<?>... defaultValue) throws IOException {
    try {
      return conf.getClasses(key, defaultValue);
    } catch (RuntimeException e) {
      Throwable c = e.getCause() != null ? e.getCause() : e;
      throw new IOException("From option " + key + ' ' + c, c);
    }
  }


  /**
   * Create an AWS credential provider from its class by using reflection.  The
   * class must implement one of the following means of construction, which are
   * attempted in order:
   *
   * <ol>
   * <li>a public constructor accepting
   *    org.apache.hadoop.conf.Configuration</li>
   * <li>a public static method named getInstance that accepts no
   *    arguments and returns an instance of
   *    com.amazonaws.auth.AWSCredentialsProvider, or</li>
   * <li>a public default constructor.</li>
   * </ol>
   *
   * @param conf configuration
   * @param credClass credential class
   * @return the instantiated class
   * @throws IOException on any instantiation failure.
   */
  public static AWSCredentialsProvider createAWSCredentialProvider(
      Configuration conf, Class<?> credClass) throws IOException {
    AWSCredentialsProvider credentials;
    String className = credClass.getName();
    if (!AWSCredentialsProvider.class.isAssignableFrom(credClass)) {
      throw new IOException("Class " + credClass + " " + NOT_AWS_PROVIDER);
    }
    if (Modifier.isAbstract(credClass.getModifiers())) {
      throw new IOException("Class " + credClass + " " + ABSTRACT_PROVIDER);
    }
    LOG.debug("Credential provider class is {}", className);

    try {
      // new X(conf)
      Constructor cons = getConstructor(credClass, Configuration.class);
      if (cons != null) {
        credentials = (AWSCredentialsProvider)cons.newInstance(conf);
        return credentials;
      }

      // X.getInstance()
      Method factory = getFactoryMethod(credClass, AWSCredentialsProvider.class,
          "getInstance");
      if (factory != null) {
        credentials = (AWSCredentialsProvider)factory.invoke(null);
        return credentials;
      }

      // new X()
      cons = getConstructor(credClass);
      if (cons != null) {
        credentials = (AWSCredentialsProvider)cons.newInstance();
        return credentials;
      }

      // no supported constructor or factory method found
      throw new IOException(String.format("%s " + CONSTRUCTOR_EXCEPTION
          + ".  A class specified in %s must provide a public constructor "
          + "accepting Configuration, or a public factory method named "
          + "getInstance that accepts no arguments, or a public default "
          + "constructor.", className, AWS_CREDENTIALS_PROVIDER));
    } catch (InvocationTargetException e) {
      Throwable targetException = e.getTargetException();
      if (targetException == null) {
        targetException =  e;
      }
      if (targetException instanceof IOException) {
        throw (IOException) targetException;
      } else {
        // supported constructor or factory method found, but the call failed
        throw new IOException(className + " " + INSTANTIATION_EXCEPTION
            + ": " + targetException,
            targetException);
      }
    } catch (ReflectiveOperationException | IllegalArgumentException e) {
      // supported constructor or factory method found, but the call failed
      throw new IOException(className + " " + INSTANTIATION_EXCEPTION
          + ": " + e,
          e);
    }
  }



  /**
   * Returns the public constructor of {@code cl} specified by the list of
   * {@code args} or {@code null} if {@code cl} has no public constructor that
   * matches that specification.
   * @param cl class
   * @param args constructor argument types
   * @return constructor or null
   */
  private static Constructor<?> getConstructor(Class<?> cl, Class<?>... args) {
    try {
      Constructor cons = cl.getDeclaredConstructor(args);
      return Modifier.isPublic(cons.getModifiers()) ? cons : null;
    } catch (NoSuchMethodException | SecurityException e) {
      return null;
    }
  }

  /**
   * Returns the public static method of {@code cl} that accepts no arguments
   * and returns {@code returnType} specified by {@code methodName} or
   * {@code null} if {@code cl} has no public static method that matches that
   * specification.
   * @param cl class
   * @param returnType return type
   * @param methodName method name
   * @return method or null
   */
  private static Method getFactoryMethod(Class<?> cl, Class<?> returnType,
      String methodName) {
    try {
      Method m = cl.getDeclaredMethod(methodName);
      if (Modifier.isPublic(m.getModifiers()) &&
          Modifier.isStatic(m.getModifiers()) &&
          returnType.isAssignableFrom(m.getReturnType())) {
        return m;
      } else {
        return null;
      }
    } catch (NoSuchMethodException | SecurityException e) {
      return null;
    }
  }
}
