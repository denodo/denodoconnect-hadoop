package com.denodo.connect.hadoop.hdfs.wrapper.util.http;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.TimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
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

    public static URI getWebHDFSOpenURI(final String host, final int port, final String user,
        final String path) throws URISyntaxException, InvalidKeyException {

        if (S3Utils.isAmazonS3(host)) {
            final String accessKey = S3Utils.getAmazonS3AccessKey(user);
            final String secretKey = S3Utils.getAmazonS3SecretKey(user);
            final String bucket = S3Utils.getAmazonS3Bucket(host);
            return getS3RESTURI(GET_METHOD, host, accessKey, secretKey, bucket, path);
        }

        return getWebHDFSURI(host, port, user, path, OPEN_OP);
    }

    public static URI getWebHDFSDeleteURI(final String host, final int port, final String user,
        final String path) throws URISyntaxException, InvalidKeyException {

        if (S3Utils.isAmazonS3(host)) {
            final String accessKey = S3Utils.getAmazonS3AccessKey(user);
            final String secretKey = S3Utils.getAmazonS3SecretKey(user);
            final String bucket = S3Utils.getAmazonS3Bucket(host);
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
    private static URI getWebHDFSURI(final String host, final int port, final String user,
        final String path, final String operation) throws URISyntaxException {

        final URIBuilder builder = new URIBuilder();
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
    public static URI getS3RESTURI(final String method, final String host, final String accessKey, final String secretKey,
        final String bucketName, final String filename) throws InvalidKeyException, URISyntaxException {

        final long secondsSinceEpoch = getOneHourLaterExpirationTime();
        final String canonicalizedResource = '/' + bucketName + filename;
        final String stringToSign = method + "\n\n\n" + secondsSinceEpoch + '\n' + canonicalizedResource;
        final String signature = sign(secretKey, stringToSign);

        return buildURI(host, filename, accessKey, secondsSinceEpoch, signature);
    }

    // set the expiration to one hour later
    private static long getOneHourLaterExpirationTime() {

        final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.HOUR, 1);
        final long secondsSinceEpoch = calendar.getTimeInMillis() / 1000L;

        return secondsSinceEpoch;
    }

    private static String sign(final String secretKey, final String stringToSign)
        throws InvalidKeyException {

        try {
            final byte[] keyBytes = secretKey.getBytes();
            final SecretKeySpec signingKey = new SecretKeySpec(keyBytes, ALGORITHM);

            final Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(signingKey);

            final byte[] digest = mac.doFinal(stringToSign.getBytes());
            final byte[] base64bytes = Base64.encodeBase64(digest);
            final String signedString = new String(base64bytes, StandardCharsets.UTF_8);

            return signedString;

        } catch (final NoSuchAlgorithmException e) {
            // never happens hopefully
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static URI buildURI(final String host, final String canonicalizedResource, final String accessKey,
        final long secondsSinceEpoch, final String signature) throws URISyntaxException {

        final URIBuilder builder = new URIBuilder();
        builder.setScheme("https").setHost(host)
            .setPath(canonicalizedResource)
            .setParameter(ACCESS_KEY_PARAMETER, accessKey)
            .setParameter(EXPIRES_PARAMETER, String.valueOf(secondsSinceEpoch))
            .setParameter(SIGNATURE_PARAMETER, signature);

        return builder.build();
    }
}
