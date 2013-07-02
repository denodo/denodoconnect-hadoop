package com.denodo.connect.hadoop.hdfs.wrapper.util.http;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.TimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.utils.URIBuilder;

import com.denodo.connect.hadoop.hdfs.wrapper.util.s3.S3Utils;

public final class URIUtils {

    private static final String REST_API_PREFIX = "/webhdfs/v1";
    private static final String OPEN_OP = "OPEN";
    private static final String DELETE_OP = "DELETE";

    private static final String GET_METHOD = "GET";
    private static final String DELETE_METHOD = "DELETE";
    private static final String ALGORITHM = "HmacSHA1";
    private static final String ACCESS_KEY_PARAMETER = "AWSAccessKeyId";
    private static final String EXPIRES_PARAMETER = "Expires";
    private static final String SIGNATURE_PARAMETER = "Signature";

    private URIUtils() {

    }

    public static URI getWebHDFSOpenURI(String host, int port, String user,
        String path) throws URISyntaxException, InvalidKeyException {

        if (S3Utils.isAmazonS3(host)) {
            String accessKey = S3Utils.getAmazonS3AccessKey(user);
            String secretKey = S3Utils.getAmazonS3SecretKey(user);
            String bucket = S3Utils.getAmazonS3Bucket(host);
            return getS3RESTURI(GET_METHOD, host, accessKey, secretKey, bucket, path);
        }

        return getWebHDFSURI(host, port, user, path, OPEN_OP);
    }

    public static URI getWebHDFSDeleteURI(String host, int port, String user,
        String path) throws URISyntaxException, InvalidKeyException {

        if (S3Utils.isAmazonS3(host)) {
            String accessKey = S3Utils.getAmazonS3AccessKey(user);
            String secretKey = S3Utils.getAmazonS3SecretKey(user);
            String bucket = S3Utils.getAmazonS3Bucket(host);
            return getS3RESTURI(DELETE_METHOD, host, accessKey, secretKey, bucket, path);
        }

        return getWebHDFSURI(host, port, user, path, DELETE_OP);
    }

    /**
     * URI for OPEN operation:
     * http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN[&user.name=<USER>]
     *
     * URI for DELETE operation:
     * http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=DELETE[&user.name=<USER>]
     */
    private static URI getWebHDFSURI(String host, int port, String user,
        String path, String operation) throws URISyntaxException {

        URIBuilder builder = new URIBuilder();
        builder.setScheme("http").setHost(host).setPort(port)
            .setPath(REST_API_PREFIX + path)
            .setParameter("op", operation);

        if (StringUtils.isNotBlank(user)) {
            builder.setParameter("user.name", user);
        }

        return builder.build();
    }

    /**
     * The URL for Amazon S3 REST access requires the authentication elements
     * to be specified as query string parameters:
     *
     * <ul>
     * <li>AWSAccessKeyId: AWS Access Key Id. Specifies the AWS Secret Access
     * Key used to sign the request, and (indirectly) the identity of the
     * developer making the request.</li>
     *
     * <li>Expires: The time when the signature expires, specified as the number of seconds since the epoch
     * (00:00:00 UTC on January 1, 1970). A request received after this time (according to the
     * server), will be rejected.</li>
     *
     * <li>Signature: The URL encoding of the Base64 encoding of the HMAC-SHA1 of StringToSign.
     *              Signature = URL-Encode(Base64(HMAC-SHA1(YourSecretAccessKeyID, UTF-8-Encoding-Of(StringToSign))));
     *              StringToSign = HTTP-VERB + "\n\n\n" + Expires + "\n" CanonicalizedResource;</li>
     * </ul>
     *
     * For more information see:
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#RESTAuthenticationQueryStringAuth
     */
    public static URI getS3RESTURI(String method, String host, String accessKey, String secretKey,
        String bucketName, String filename) throws InvalidKeyException, URISyntaxException {

        long secondsSinceEpoch = getOneHourLaterExpirationTime();
        String canonicalizedResource = "/" + bucketName + filename;
        String stringToSign = method + "\n\n\n" + secondsSinceEpoch + "\n" + canonicalizedResource;
        String signature = sign(secretKey, stringToSign);

        return buildURI(host, filename, accessKey, secondsSinceEpoch, signature);
    }

    // set the expiration to one hour later
    private static long getOneHourLaterExpirationTime() {

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.HOUR, 1);
        long secondsSinceEpoch = calendar.getTimeInMillis() / 1000L;

        return secondsSinceEpoch;
    }

    private static String sign(String secretKey, String stringToSign)
        throws InvalidKeyException {

        try {
            byte[] keyBytes = secretKey.getBytes();
            SecretKeySpec signingKey = new SecretKeySpec(keyBytes, ALGORITHM);

            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(signingKey);

            byte[] digest = mac.doFinal(stringToSign.getBytes());
            byte[] base64bytes = Base64.encodeBase64(digest);
            String signedString = new String(base64bytes, "UTF-8");

            return signedString;

        } catch (NoSuchAlgorithmException e) {
            // never happens hopefully
            throw new IllegalStateException(e.getMessage(), e);
        } catch (UnsupportedEncodingException e) {
            // never happens hopefully
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static URI buildURI(String host, String canonicalizedResource, String accessKey,
        long secondsSinceEpoch, String signature) throws URISyntaxException {

        URIBuilder builder = new URIBuilder();
        builder.setScheme("https").setHost(host)
            .setPath(canonicalizedResource)
            .setParameter(ACCESS_KEY_PARAMETER, accessKey)
            .setParameter(EXPIRES_PARAMETER, String.valueOf(secondsSinceEpoch))
            .setParameter(SIGNATURE_PARAMETER, signature);

        return builder.build();
    }
}
