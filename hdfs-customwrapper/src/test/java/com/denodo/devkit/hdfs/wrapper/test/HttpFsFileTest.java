package com.denodo.devkit.hdfs.wrapper.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;

import com.denodo.vdb.engine.customwrapper.CustomWrapperException;

public class HttpFsFileTest {

    public static void main(String[] args) throws Exception {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        // HttpClientUtils.setProxyIfValid(httpClient, proxyDetails);

        // Construct
        HttpRequestBase httpRequest = new HttpGet();
        // Set request uri
        // URL has the form http://<HOST>:<HTTP_PORT>/webhdfs/v1/<PATH>?op=..

        String host = "192.168.73.132";
        int port = 14000;
        String prefix = "/webhdfs/v1";
        String path = "/wordcount/output/part-r-00000";
        String username = "cloudera";
        String operation = "OPEN";
        int offset = 0;
        int len = 1024;
        httpRequest.setURI(URI.create("http://" + host + ":" + port + prefix
                + path + "?user.name=" + username + "&op=" + operation));

        // Execute
        try {
            org.apache.http.HttpResponse response = httpClient
                    .execute(httpRequest);
            HttpEntity responseEntity = response.getEntity();
            InputStream is = responseEntity.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = br.readLine()) != null) {
                String[] line_array = line.split("\t");
                // column_delimiter matches the key/value delimiter
                if (line_array.length == 2) {
                    printArray(line_array);
                } else {
                    System.out
                            .println("Column delimiter does not match the key/value delimiter");
                    throw new CustomWrapperException(
                            "Column delimiter does not match the key/value delimiter");
                }
            }

        } catch (ClientProtocolException e) {
            throw new Exception(httpRequest.getURI().toString(), e);
        } catch (IOException e) {
            throw new Exception(httpRequest.getURI().toString(), e);
        }
    }

    public static void printArray(String[] array) {
        for (String s : array)
            System.out.print(s + " ");
        System.out.println();
    }
}
