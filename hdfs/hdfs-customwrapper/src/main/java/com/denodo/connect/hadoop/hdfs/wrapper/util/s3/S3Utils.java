package com.denodo.connect.hadoop.hdfs.wrapper.util.s3;

import org.apache.commons.lang.StringUtils;


public final class S3Utils {

    private static final String AMAZON_WEB_SERVICE = "amazonaws.com";


    private S3Utils() {

    }

    public static boolean isAmazonS3(String host) {
        return host.contains(AMAZON_WEB_SERVICE);
    }

    public static String getAmazonS3Bucket(String host) {

        if (!host.contains(".")) {
            throw new IllegalArgumentException("Host should be of the form <bucket>.s3.amazonaws.com or <bucket>.s3<region>.amazonaws.com");
        }

        return StringUtils.substringBefore(host, ".");
    }

    public static String getAmazonS3AccessKey(String user) {

        if (!user.contains(":")) {
            throw new IllegalArgumentException("User should be of the form <accessKey>:<secretKey>");
        }

        return StringUtils.substringBefore(user, ":");
    }

    public static String getAmazonS3SecretKey(String user) {

        if (!user.contains(":")) {
            throw new IllegalArgumentException("User should be of the form <accessKey>:<secretKey>");
        }

        return StringUtils.substringAfter(user, ":");
    }
}
