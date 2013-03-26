package com.denodo.connect.hadoop.hdfs.util.classloader;

import org.apache.hadoop.conf.Configuration;


public final class ClassLoaderUtils {

    private ClassLoaderUtils() {

    }

    public static ClassLoader changeContextClassLoader() {
        ClassLoader originalCtxClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(Configuration.class.getClassLoader());
        return originalCtxClassLoader;
    }

    public static void restoreContextClassLoader(ClassLoader originalCtxClassLoader) {
        Thread.currentThread().setContextClassLoader(originalCtxClassLoader);
    }
}
