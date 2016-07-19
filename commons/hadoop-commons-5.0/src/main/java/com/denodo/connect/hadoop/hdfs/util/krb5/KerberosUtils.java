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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.commons.auth.LoginConfig;


public final class KerberosUtils {

    private static final Logger logger = Logger.getLogger(KerberosUtils.class);

    private KerberosUtils() {

    }

    public static void enableKerberos() {
        
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hadoop.security.auth_to_local",  "RULE:[1:$1] RULE:[2:$1]"); // just extract the simple user name (for quickstart.cloudera)
        UserGroupInformation.setConfiguration(conf);
    }

    public static UserGroupInformation loginFromTicketCache() throws IOException {

        setupCommonLoginProperties(null, null);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        return ugi;
    }
    
    public static UserGroupInformation loginFromKeytab(String principal, String kdc, String keytabPath) throws IOException {

        setupCommonLoginProperties(kdc, principal);
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
        return ugi;
    }

    public static UserGroupInformation loginFromPassword(final String principal, String kdc, final String password) throws IOException {

        try {

            setupCommonLoginProperties(kdc, principal);
            
            LoginContext loginContext = new LoginContext("", null, new CallbackHandler() {
                @Override
                public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                  for(Callback c : callbacks){
                    if(c instanceof NameCallback) {
                        ((NameCallback) c).setName(principal);
                    }
                    if(c instanceof PasswordCallback) {
                        ((PasswordCallback) c).setPassword(password.toCharArray());
                    }
                  }
               }}, new LoginConfig());
            
            loginContext.login();
            
            return UserGroupInformation.getUGIFromSubject(loginContext.getSubject());

        } catch (LoginException e) {
            logger.debug("Login error", e);
            throw new IOException("Login error", e);
        }
    }
    
    private static void setupCommonLoginProperties(String kdc, String principal) {

        if (StringUtils.isNotBlank(kdc)) {
            System.setProperty("java.security.krb5.kdc", kdc);
            System.setProperty("java.security.krb5.realm", getRealm(principal));
        }

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
        // Previously it cleared Kerberos credentials information but for safety reasons in concurrent environments
        // now it does nothing. See redmine #28268.

    }

}
