package com.denodo.connect.hadoop.hdfs.commons.auth;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

public class LoginConfig extends Configuration {


	public LoginConfig() {
		super();
	}

	@Override
	public AppConfigurationEntry[] getAppConfigurationEntry(String name) {

	    Map<String, String> options = new HashMap<String, String>();
	    
	    // Is is set to true the configuration values will be refreshed before the login method of the Krb5LoginModule is called.
	    // When switching Kerberos configurations, it is REQUIRED that refreshKrb5Config should be set to true. 
	    // Failure to set this value can lead to unexpected results.
	    options.put("refreshKrb5Config", "true");
	    
		return new AppConfigurationEntry[] { new AppConfigurationEntry(
				"com.sun.security.auth.module.Krb5LoginModule",
				AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
				options) };
	}

}