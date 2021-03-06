/*
 * =============================================================================
 *
 *   This software is part of the denodo developer toolkit.
 *
 *   Copyright (c) 2014, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.dfs.wrapper;

import java.io.InputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.dfs.commons.naming.Parameter;
import com.denodo.connect.dfs.util.configuration.ConfigurationUtils;
import com.denodo.connect.dfs.util.krb5.KerberosUtils;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterLocalRouteValue;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterRouteValue;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterValue;


public abstract class AbstractSecureWrapper extends AbstractCustomWrapper {

    private static final  Logger LOG = LoggerFactory.getLogger(AbstractSecureWrapper.class);

    private boolean securityEnabled;
    private String userPrincipal;
    private boolean stopRequested;

    private static final CustomWrapperInputParameter[] SECURE_DATA_SOURCE_INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.KERBEROS_ENABLED,
                "Is Kerberos enabled? ",
                false, true, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.PRINCIPAL,
                "Kerberos v5 Principal name to access DFS, e.g. primary/instance@realm ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.KEYTAB,
                "Keytab file containing the key of the Kerberos principal ",
                false, true, CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL})),
            new CustomWrapperInputParameter(Parameter.KERBEROS_PASWORD,
                "Password associated with the principal ",
                false, true, CustomWrapperInputParameterTypeFactory.passwordType()),
            new CustomWrapperInputParameter(Parameter.KDC,
                "Kerberos Key Distribution Center ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType())
    };

    public AbstractSecureWrapper() {
        this.securityEnabled = false;
        this.userPrincipal = null;
        this.stopRequested = false;
    }

    @Override
    public CustomWrapperInputParameter[] getDataSourceInputParameters() {
        return SECURE_DATA_SOURCE_INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(final Map<String, String> inputValues)
        throws CustomWrapperException {

        try {
            setSecurityEnabled(inputValues);
            checkConfig(inputValues);
            final UserGroupInformation ugi = login(inputValues);
            if (ugi == null) {
                return doGetSchemaParameters(inputValues);
            }
            
            return ugi.doAs(new PrivilegedExceptionAction<CustomWrapperSchemaParameter[]>() {
                @Override
                public CustomWrapperSchemaParameter[] run() throws Exception {
                    
                        return doGetSchemaParameters(inputValues);
                }
            });
        } catch (final UndeclaredThrowableException e) {
            LOG.error("Error running the wrapper ", e);
            final Exception ex = (Exception) e.getCause();
            throw new CustomWrapperException(ex.getLocalizedMessage(), ex);            
        } catch (final Exception e) {
            throw new CustomWrapperException(e.getLocalizedMessage(), e);
        }

    }

    @Override
    public void run(final CustomWrapperConditionHolder condition,
        final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperResult result, final Map<String, String> inputValues)
        throws CustomWrapperException {

        try {

            setSecurityEnabled(inputValues);
            final UserGroupInformation ugi = login(inputValues);
            if (ugi == null) {
                doRun(condition, projectedFields, result, inputValues);
            } else {
                ugi.doAs(new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws Exception {
                        doRun(condition, projectedFields, result, inputValues);
                        return null;
                    }
                });
            }
        } catch (final CustomWrapperException e) {
            throw e;
        } catch (final UndeclaredThrowableException e) {
            LOG.error("Error running the wrapper ", e);
            final Exception ex = (Exception) e.getCause();
            throw new CustomWrapperException(ex.getLocalizedMessage(), ex);
        } catch(final Exception e) {
            LOG.error("Error running the wrapper ", e);
            throw new CustomWrapperException(e.getLocalizedMessage(), e);
        }

    }

    private static void checkConfig(final Map<String, String> inputValues) {
        
        if (inputValues.get(Parameter.PRINCIPAL) != null) {

            String errorMsg = null;
            final String keytab = inputValues.get(Parameter.KEYTAB);
            final String password = inputValues.get(Parameter.KERBEROS_PASWORD);
            if (StringUtils.isBlank(keytab) && StringUtils.isBlank(password)) {
                errorMsg = "One of these parameters: '" + Parameter.KEYTAB + "' or '" + Parameter.KERBEROS_PASWORD + "' must be specified";
            }

            if (StringUtils.isNotBlank(errorMsg)) {
                throw new IllegalArgumentException(errorMsg);
            }
        }

    }

    /*
     * Two ways for login to a kerberized Hadoop cluster:
     * - Using a Kerberos ticket from the machine this wrapper is running on.
     * - Using the parameters provided by the user to login in Kerberos programmatically.
     */
    private UserGroupInformation login(final Map<String, String> inputValues) throws CustomWrapperException {

        try {
            UserGroupInformation ugi = null;
            if (isSecurityEnabled()) {
                this.userPrincipal = inputValues.get(Parameter.PRINCIPAL);

                if (loginWithKerberosTicket()) {
                    ugi = KerberosUtils.loginFromTicketCache();
                } else {

                    final String kdc = inputValues.get(Parameter.KDC);
                    if (inputValues.get(Parameter.KEYTAB) != null) {
                        final String keytabPath = ((CustomWrapperInputParameterLocalRouteValue) getInputParameterValue(Parameter.KEYTAB)).getPath();
                        ugi = KerberosUtils.loginFromKeytab(this.userPrincipal, kdc, keytabPath);
                    } else if (inputValues.get(Parameter.KERBEROS_PASWORD) != null) {
                        final String password = inputValues.get(Parameter.KERBEROS_PASWORD);
                        ugi = KerberosUtils.loginFromPassword(this.userPrincipal, kdc, password);
                    }
                }
            } else {
                KerberosUtils.disableKerberos();
            }
            
            return ugi;

        } catch (final Exception e) {
            LOG.error("Hadoop security error", e);
            String msg = "Hadoop security error: " + e.getMessage();
            if (e.getCause() != null) {
                msg += ' ' + e.getCause().getMessage();
            }
            throw new CustomWrapperException(msg, e);
        }
    }

    private boolean loginWithKerberosTicket() {
        return this.userPrincipal == null;
    }

    private void setSecurityEnabled(final Map<String, String> inputValues) {

        final String kerberosEnabled = inputValues.get(Parameter.KERBEROS_ENABLED);
        if (Boolean.parseBoolean(kerberosEnabled)) {
            this.securityEnabled = true;
        }
    }

    private boolean isSecurityEnabled() {
        return this.securityEnabled;
    }

    public String getUserPrincipal() {
        return this.userPrincipal;
    }
    
    public boolean isStopRequested() {
        return this.stopRequested;
    }
    
    @Override
    public boolean stop() {
        this.stopRequested = true;
        return true;
    }

    protected Configuration getConfiguration(final Map<String, String> inputValues) throws CustomWrapperException {

        final String fileSystemURI = StringUtils.trim(inputValues.get(Parameter.FILESYSTEM_URI));

        final CustomWrapperInputParameterValue coreSitePathValue = getInputParameterValue(Parameter.CORE_SITE_PATH);
        InputStream coreSiteIs = null;
        if (coreSitePathValue != null) {
            coreSiteIs = ((CustomWrapperInputParameterRouteValue) coreSitePathValue).getInputStream();

        }

        final CustomWrapperInputParameterValue dfsSitePathValue = getInputParameterValue(Parameter.HDFS_SITE_PATH);
        InputStream dfsSiteIs = null;
        if (dfsSitePathValue != null) {
            dfsSiteIs = ((CustomWrapperInputParameterRouteValue) dfsSitePathValue).getInputStream();

        }

        return ConfigurationUtils.getConfiguration(fileSystemURI, coreSiteIs, dfsSiteIs);
    }

    public abstract void doRun(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException;

    public abstract CustomWrapperSchemaParameter[] doGetSchemaParameters(
        Map<String, String> inputValues) throws CustomWrapperException;
}
