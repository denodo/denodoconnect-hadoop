/*
 *
 * Copyright (c) 2019. DENODO Technologies.
 * http://www.denodo.com
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of DENODO
 * Technologies ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with DENODO.
 *
 */
package com.denodo.connect.hadoop.hdfs.wrapper.providers;

import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_ARN;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_SESSION_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_SESSION_DURATION_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_SESSION_NAME;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_STS_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION_DEFAULT;
import static org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider;
import static org.apache.hadoop.fs.s3a.S3AUtils.loadAWSProviderClasses;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3ARetryPolicy;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.STSClientFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException;
import com.google.common.annotations.VisibleForTesting;

public class AWSAssumedRoleCredentialProvider implements AWSCredentialsProvider, Closeable {

    private static final Logger LOG =
        LoggerFactory.getLogger(AWSAssumedRoleCredentialProvider.class);
    public static final String NAME
        = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider";

    static final String E_FORBIDDEN_PROVIDER =
        "AssumedRoleCredentialProvider cannot be in "
            + ASSUMED_ROLE_CREDENTIALS_PROVIDER;

    public static final String E_NO_ROLE = "Unset property "
        + ASSUMED_ROLE_ARN;

    private final STSAssumeRoleSessionCredentialsProvider stsProvider;

    private final String sessionName;

    private final long duration;

    private final String arn;

    private final AWSCredentialProviderList credentialsToSTS;

    private final Invoker invoker;

    /**
     * Instantiate.
     * This calls {@link #getCredentials()} to fail fast on the inner
     * role credential retrieval.
     * @param fsUri URI of the filesystem.
     * @param conf configuration
     * @throws IOException on IO problems and some parameter checking
     * @throws IllegalArgumentException invalid parameters
     * @throws AWSSecurityTokenServiceException problems getting credentials
     */
    public AWSAssumedRoleCredentialProvider(final URI fsUri, final Configuration conf)
        throws IOException {

        this.arn = conf.getTrimmed(ASSUMED_ROLE_ARN, "");
        if (StringUtils.isEmpty(this.arn)) {
            throw new IOException(E_NO_ROLE);
        }

        // build up the base provider
        final Class<?>[] awsClasses = loadAWSProviderClasses(conf,
            ASSUMED_ROLE_CREDENTIALS_PROVIDER,
            SimpleAWSCredentialsProvider.class);
        this.credentialsToSTS = new AWSCredentialProviderList();
        for (final Class<?> aClass : awsClasses) {
            if (this.getClass().equals(aClass)) {
                throw new IOException(E_FORBIDDEN_PROVIDER);
            }
            this.credentialsToSTS.add(createAWSCredentialProvider(conf, aClass, fsUri));
        }
        LOG.debug("Credentials to obtain role credentials: {}", this.credentialsToSTS);

        // then the STS binding
        this.sessionName = conf.getTrimmed(ASSUMED_ROLE_SESSION_NAME,
            buildSessionName());
        this.duration = conf.getTimeDuration(ASSUMED_ROLE_SESSION_DURATION,
            ASSUMED_ROLE_SESSION_DURATION_DEFAULT, TimeUnit.SECONDS);
        final String policy = conf.getTrimmed(ASSUMED_ROLE_POLICY, "");

        LOG.debug("{}", this);
        final STSAssumeRoleSessionCredentialsProvider.Builder builder
            = new STSAssumeRoleSessionCredentialsProvider.Builder(this.arn, this.sessionName);
        builder.withRoleSessionDurationSeconds((int) this.duration);
        if (StringUtils.isNotEmpty(policy)) {
            LOG.debug("Scope down policy {}", policy);
            builder.withScopeDownPolicy(policy);
        }
        final String endpoint = conf.get(ASSUMED_ROLE_STS_ENDPOINT, "");
        final String region = conf.get(ASSUMED_ROLE_STS_ENDPOINT_REGION,
            ASSUMED_ROLE_STS_ENDPOINT_REGION_DEFAULT);
        final AWSSecurityTokenServiceClientBuilder stsbuilder =
            STSClientFactory.builder(
                conf,
                fsUri.getHost(),
                this.credentialsToSTS,
                endpoint,
                region).withRegion(region);
        // the STS client is not tracked for a shutdown in close(), because it
        // (currently) throws an UnsupportedOperationException in shutdown().
        builder.withStsClient(stsbuilder.build());

        //now build the provider
        this.stsProvider = builder.build();

        // to handle STS throttling by the AWS account, we
        // need to retry

        invoker = new Invoker(new S3ARetryPolicy(conf), new Invoker.Retried() {
            @Override
            public void onFailure(final String text, final IOException exception, final int retries, final boolean idempotent) {
                AWSAssumedRoleCredentialProvider.this.operationRetried(text, exception, retries, idempotent);
            }
        });


        // and force in a fail-fast check just to keep the stack traces less
        // convoluted
        getCredentials();
    }

    /**
     * Get credentials.
     * @return the credentials
     * @throws AWSSecurityTokenServiceException if none could be obtained.
     */
    @Override
    public AWSCredentials getCredentials() {
        try {
            return this.invoker.retryUntranslated("getCredentials",
                true,

                new Invoker.Operation<AWSCredentials>() {
                    @Override
                    public AWSCredentials execute() throws IOException {
                        return AWSAssumedRoleCredentialProvider.this.stsProvider.getCredentials();
                    }
                });
        } catch (IOException e) {

            // this is in the signature of retryUntranslated;
            // its hard to see how this could be raised, but for
            // completeness, it is wrapped as an Amazon Client Exception
            // and rethrown.
            throw new AmazonClientException(
                "getCredentials failed: " + e,
                e);
        } catch (final AWSSecurityTokenServiceException e) {
            LOG.error("Failed to get credentials for role {}",
                this.arn, e);
            throw e;
        }
    }

    @Override
    public void refresh() {
        this.stsProvider.refresh();
    }

    /**
     * Propagate the close() call to the inner stsProvider.
     */
    @Override
    public void close() {
        S3AUtils.closeAutocloseables(LOG, this.stsProvider, this.credentialsToSTS);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(
            "AssumedRoleCredentialProvider{");
        sb.append("role='").append(this.arn).append('\'');
        sb.append(", session'").append(this.sessionName).append('\'');
        sb.append(", duration=").append(this.duration);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Build the session name from the current user's shortname.
     * @return a string for the session name.
     * @throws IOException failure to get the current user
     */
    static String buildSessionName() throws IOException {
        return sanitize(UserGroupInformation.getCurrentUser()
            .getShortUserName());
    }

    /**
     * Build a session name from the string, sanitizing it for the permitted
     * characters.
     * @param session source session
     * @return a string for use in role requests.
     */
    @VisibleForTesting
    static String sanitize(final String session) {
        final StringBuilder r = new StringBuilder(session.length());
        for (final char c: session.toCharArray()) {
            if ("abcdefghijklmnopqrstuvwxyz0123456789,.@-".contains(
                Character.toString(c).toLowerCase(Locale.ENGLISH))) {
                r.append(c);
            } else {
                r.append('-');
            }
        }
        return r.toString();
    }

    /**
     * Callback from {@link Invoker} when an operation is retried.
     * @param text text of the operation
     * @param ex exception
     * @param retries number of retries
     * @param idempotent is the method idempotent
     */
    public void operationRetried(
        final String text,
        final Exception ex,
        final int retries,
        final boolean idempotent) {
        if (retries == 0) {
            // log on the first retry attempt of the credential access.
            // At worst, this means one log entry every intermittent renewal
            // time.
            LOG.info("Retried {}", text);
        }
    }
}
