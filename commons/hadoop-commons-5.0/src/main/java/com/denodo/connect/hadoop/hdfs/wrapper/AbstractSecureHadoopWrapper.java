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
package com.denodo.connect.hadoop.hdfs.wrapper;

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.util.krb5.KerberosUtils;
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


public abstract class AbstractSecureHadoopWrapper extends AbstractCustomWrapper {

    private static final Logger logger = Logger.getLogger(AbstractSecureHadoopWrapper.class);

    private boolean securityEnabled;
    private String userPrincipal;
    private boolean stopRequested;

    private static final CustomWrapperInputParameter[] SECURE_INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.KERBEROS_ENABLED,
                "Is Kerberos enabled? ", false,
                CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.PRINCIPAL,
                "Kerberos v5 Principal name to access HDFS, e.g. primary/instance@realm ", false,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.KEYTAB,
                "Keytab file containing the key of the Kerberos principal ", false,
                CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL})),
            new CustomWrapperInputParameter(Parameter.KERBEROS_PASWORD,
                "Password associated with the principal ", false,
                CustomWrapperInputParameterTypeFactory.passwordType()),
            new CustomWrapperInputParameter(Parameter.KDC,
                "Kerberos Key Distribution Center ", false,
                CustomWrapperInputParameterTypeFactory.stringType())
    };

    public AbstractSecureHadoopWrapper() {
        this.securityEnabled = false;
        this.userPrincipal = null;
        this.stopRequested = false;
    }

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return SECURE_INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(final Map<String, String> inputValues)
        throws CustomWrapperException {

        try {
            setSecurityEnabled(inputValues);
            checkConfig(inputValues);
            UserGroupInformation ugi = login(inputValues);
            if (ugi == null) {
                return doGetSchemaParameters(inputValues);
            }
            
            return ugi.doAs(new PrivilegedExceptionAction<CustomWrapperSchemaParameter[]>() {
                @Override
                public CustomWrapperSchemaParameter[] run() throws Exception {
                    return doGetSchemaParameters(inputValues);
                }
            });
        } catch (Exception e) {
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
            UserGroupInformation ugi = login(inputValues);
            if (ugi == null) {
                doRun(condition, projectedFields, result, inputValues);
            } else {
                ugi.doAs(new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws Exception {
                        doRun(condition, projectedFields, result, inputValues);
                        return null;
                    }
                } );
            }
        } catch (UndeclaredThrowableException e) {
            logger.error("Error running the wrapper ", e);
            Exception ex = (Exception) e.getCause();
            throw new CustomWrapperException(ex.getLocalizedMessage(), ex);
        } catch(Exception e) {
            logger.error("Error running the wrapper ", e);
            throw new CustomWrapperException(e.getLocalizedMessage(), e);
        }

    }

    private static void checkConfig(Map<String, String> inputValues) {
        
        if (inputValues.get(Parameter.PRINCIPAL) != null) {

            String errorMsg = null;
            String keytab = inputValues.get(Parameter.KEYTAB);
            String password = inputValues.get(Parameter.KERBEROS_PASWORD);
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
    private UserGroupInformation login(Map<String, String> inputValues) throws CustomWrapperException {

        try {
            UserGroupInformation ugi = null;
            if (isSecurityEnabled()) {
                this.userPrincipal = inputValues.get(Parameter.PRINCIPAL);

                if (loginWithKerberosTicket()) {
                    ugi = KerberosUtils.loginFromTicketCache();
                } else {

                    String kdc = inputValues.get(Parameter.KDC);
                    if (inputValues.get(Parameter.KEYTAB) != null) {
                        String keytabPath = ((CustomWrapperInputParameterLocalRouteValue) getInputParameterValue(Parameter.KEYTAB)).getPath();
                        ugi = KerberosUtils.loginFromKeytab(this.userPrincipal, kdc, keytabPath);
                    } else if (inputValues.get(Parameter.KERBEROS_PASWORD) != null) {
                        String password = inputValues.get(Parameter.KERBEROS_PASWORD);
                        ugi = KerberosUtils.loginFromPassword(this.userPrincipal, kdc, password);
                    }
                }
            }
            
            return ugi;

        } catch (Exception e) {
            logger.error("Hadoop security error", e);
            String msg = "Hadoop security error: " + e.getMessage();
            if (e.getCause() != null) {
                msg += " " + e.getCause().getMessage();
            }
            throw new CustomWrapperException(msg, e);
        }
    }

    public boolean loginWithKerberosTicket() {
        return this.userPrincipal == null;
    }

    private void setSecurityEnabled(Map<String, String> inputValues) {

        String kerberosEnabled = inputValues.get(Parameter.KERBEROS_ENABLED);
        if (Boolean.parseBoolean(kerberosEnabled)) {
            this.securityEnabled = true;
        }
    }

    public boolean isSecurityEnabled() {
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

    public abstract void doRun(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException;

    public abstract CustomWrapperSchemaParameter[] doGetSchemaParameters(
        Map<String, String> inputValues) throws CustomWrapperException;
}
