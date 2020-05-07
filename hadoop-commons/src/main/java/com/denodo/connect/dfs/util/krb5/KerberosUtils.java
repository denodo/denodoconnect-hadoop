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
package com.denodo.connect.dfs.util.krb5;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.dfs.commons.auth.LoginConfig;


public final class KerberosUtils {

    private static final  Logger LOG = LoggerFactory.getLogger(KerberosUtils.class);

    private KerberosUtils() {

    }

    private static void enableKerberos() {
        
        final Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hadoop.security.auth_to_local",  "RULE:[1:$1] RULE:[2:$1]"); // just extract the simple user name (for quickstart.cloudera)
        UserGroupInformation.setConfiguration(conf);
    }
    
    public static void disableKerberos() {
        
        final Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "simple");
        UserGroupInformation.setConfiguration(conf);
    }

    public static UserGroupInformation loginFromTicketCache() throws IOException {

        setupCommonLoginProperties(null, null);
        final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        return ugi;
    }
    
    public static UserGroupInformation loginFromKeytab(final String principal, final String kdc, final String keytabPath) throws IOException {

        setupCommonLoginProperties(kdc, principal);
        final UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
        return ugi;
    }

    public static UserGroupInformation loginFromPassword(final String principal, final String kdc, final String password) throws IOException {

        try {

            setupCommonLoginProperties(kdc, principal);
            
            final LoginContext loginContext = new LoginContext("", null, new CallbackHandler() {
                @Override
                public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                  for(final Callback c : callbacks){
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

        } catch (final LoginException e) {
            LOG.debug("Login error", e);
            throw new IOException("Login error", e);
        }
    }
    
    private static void setupCommonLoginProperties(final String kdc, final String principal) {

        if (StringUtils.isNotBlank(kdc)) {
            System.setProperty("java.security.krb5.kdc", kdc);
            System.setProperty("java.security.krb5.realm", getRealm(principal));
        }

        enableKerberos();

     }


    /* Principal is of the form 'primary/instance@realm', being optional the 'instance' component. */
    private static String getRealm(final String principal) {

        final String[] components = principal.split("@");
        if (components.length == 1) {
            throw new IllegalArgumentException("Kerberos v5 Principal name is of the form primary/instance@realm: primary and realm are mandatory.");
        }
        final String realm = components[components.length - 1];
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
