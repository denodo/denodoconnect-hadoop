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
package com.denodo.connect.hadoop.hdfs.util.krb5;

import java.io.IOException;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.commons.auth.LoginConfig;
import com.denodo.connect.hadoop.hdfs.commons.auth.PasswordCallbackHandler;


public final class KerberosUtils {

    private static final Logger logger = Logger.getLogger(KerberosUtils.class);

    private KerberosUtils() {

    }

    public static void enableKerberos() {
        
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hadoop.security.auth_to_local",  "RULE:[1:$1] RULE:[2:$1]");
        UserGroupInformation.setConfiguration(conf);
    }

    public static void loginFromKeytab(String principal, String kdc, String keytabPath) throws IOException {

            setupCommonLoginProperties(principal, kdc);
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
    }

    public static void loginFromPassword(String principal, String kdc, String password) throws IOException {

        try {

            setupCommonLoginProperties(principal, kdc);
            System.setProperty("sun.security.krb5.principal", principal);

            LoginContext loginContext = new LoginContext("", null,
                new PasswordCallbackHandler(password), new LoginConfig());
            loginContext.login();

            UserGroupInformation.loginUserFromSubject(loginContext.getSubject());

        } catch (LoginException e) {
            logger.debug("Login error", e);
            throw new IOException("Login error", e);
        }
    }
    
    private static void setupCommonLoginProperties(String principal, String kdc) {

         System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
         System.setProperty("java.security.krb5.realm", getRealm(principal));
         System.setProperty("java.security.krb5.kdc", kdc);
         
         enableKerberos();

     }


    /* Principal is of the form 'primary/instance@realm', being optional the 'instance' component. */
    public static String getRealm(String principal) {

        String[] components = principal.split("@");
        if (components.length == 1) {
            throw new IllegalArgumentException("Kerberos v5 Principal name is of the form primary/instance@realm: primary and realm are mandatory.");
        }
        String realm = components[components.length - 1];
        if (StringUtils.isBlank(realm)) {
            throw new IllegalArgumentException("Kerberos v5 Principal name is of the form primary/instance@realm: realm is mandatory.");
        }

        return realm;

    }

    public static void logout() {
        
        System.clearProperty("java.security.krb5.realm");
        System.clearProperty("java.security.krb5.kdc");
        System.clearProperty("sun.security.krb5.principal");
                
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "simple");
        UserGroupInformation.setConfiguration(conf);
        
        UserGroupInformation.setLoginUser(null);

    }

}
