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

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
    }

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return SECURE_INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(Map<String, String> inputValues)
        throws CustomWrapperException {

        try {
            setSecurityEnabled(inputValues);
            checkConfig(inputValues);
            login(inputValues);

            return doGetSchemaParameters(inputValues);

        } finally {
            logout();
        }

    }

    @Override
    public void run(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException {

        try {

            setSecurityEnabled(inputValues);
            login(inputValues);
            
            doRun(condition, projectedFields, result, inputValues);

        } finally {
            logout();
        }
    }

    private static void checkConfig(Map<String, String> inputValues) {
        
        if (inputValues.get(Parameter.PRINCIPAL) != null) {

            StringBuilder errorSB = new StringBuilder();
            String kdc = inputValues.get(Parameter.KDC);
            if (StringUtils.isBlank(kdc)) {
                errorSB.append(Parameter.KDC).append(" is required\n");
            }
            String keytab = inputValues.get(Parameter.KEYTAB);
            String password = inputValues.get(Parameter.KERBEROS_PASWORD);
            if (StringUtils.isBlank(keytab) && StringUtils.isBlank(password)) {
                errorSB.append("One of these parameters: '"
                        + Parameter.KEYTAB + "' or '" + Parameter.KERBEROS_PASWORD + "' must be specified");
            }

            String errorMsg = errorSB.toString();
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
    private void login(Map<String, String> inputValues) throws CustomWrapperException {

        try {
            if (isSecurityEnabled()) {
                this.userPrincipal = inputValues.get(Parameter.PRINCIPAL);

                if (loginWithKerberosTicket()) {
                    KerberosUtils.enableKerberos();
                } else {

                    String kdc = inputValues.get(Parameter.KDC);
                    if (inputValues.get(Parameter.KEYTAB) != null) {
                        String keytabPath = ((CustomWrapperInputParameterLocalRouteValue) getInputParameterValue(Parameter.KEYTAB)).getPath();
                        KerberosUtils.loginFromKeytab(this.userPrincipal, kdc, keytabPath);
                    } else if (inputValues.get(Parameter.KERBEROS_PASWORD) != null) {
                        String password = inputValues.get(Parameter.KERBEROS_PASWORD);
                        KerberosUtils.loginFromPassword(this.userPrincipal, kdc, password);
                    }
                }
            }

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
    
    /* 
     * When switching Kerberos configurations, the logout is REQUIRED because Hadoop caches some information between logins.
     */
    private void logout() {
       
        if (isSecurityEnabled()) {
            KerberosUtils.logout();
        }
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

    public abstract void doRun(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException;

    public abstract CustomWrapperSchemaParameter[] doGetSchemaParameters(
        Map<String, String> inputValues) throws CustomWrapperException;
}