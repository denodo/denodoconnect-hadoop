/*
 * =============================================================================
 * 
 *   This software is part of the DenodoConnect component collection.
 *   
 *   Copyright (c) 2012, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.hdfs.test;

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

        String host = "192.168.25.128";
        int port = 50075;
        String prefix = "/webhdfs/v1";
        String path = "/user/sandbox/text";
        String username = "sandbox";
        String operation = "OPEN";
        int offset = 0;
        int len = 1024;
        httpRequest.setURI(URI.create("http://" + host + ":" + port + prefix + path + "?user.name=" + username + "&op=" + operation));

        // Execute
        try {
            org.apache.http.HttpResponse response = httpClient.execute(httpRequest);
            HttpEntity responseEntity = response.getEntity();
            InputStream is = responseEntity.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = br.readLine()) != null) {
                String[] lineArray = line.split("\t");
                // columnDelimiter matches the key/value delimiter
                if (lineArray.length == 2) {
                    printArray(lineArray);
                } else {
                    System.out.println("Column delimiter does not match the key/value delimiter");
                    throw new CustomWrapperException("Column delimiter does not match the key/value delimiter");
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
