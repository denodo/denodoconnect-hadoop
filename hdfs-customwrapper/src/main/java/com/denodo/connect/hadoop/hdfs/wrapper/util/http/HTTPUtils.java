package com.denodo.connect.hadoop.hdfs.wrapper.util.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;


public final class HTTPUtils {

    private HTTPUtils() {

    }

    public static InputStream requestGet(final URI uri, final DefaultHttpClient httpClient) throws IOException {

        final HttpRequestBase httpRequest = new HttpGet(uri);
        return sendRequest(httpClient, httpRequest);

    }

    public static void requestDelete(final URI uri, final DefaultHttpClient httpClient) throws IOException {

        final HttpRequestBase httpRequest = new HttpDelete(uri);
        sendRequest(httpClient, httpRequest);
    }

    private static InputStream sendRequest(final DefaultHttpClient httpClient,
        final HttpRequestBase httpRequest) throws IOException {

        final HttpResponse response = httpClient.execute(httpRequest);
        final int statusCode = response.getStatusLine().getStatusCode();
        final HttpEntity responseEntity = response.getEntity();
        if (responseEntity != null) {
            final InputStream is = responseEntity.getContent();
            if (statusCode == HttpStatus.SC_OK) {
                return is;
            }
            final String statusMessage = response.getStatusLine().getReasonPhrase();
            final String statusResponse = IOUtils.toString(is);
            throw new IOException("HTTP error code " + statusCode + ". " + statusMessage + ": "
                + statusResponse);
        }

        return null;
    }


}
