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
	    options.put("refreshKrb5Config", "true");
	    
		return new AppConfigurationEntry[] { new AppConfigurationEntry(
				"com.sun.security.auth.module.Krb5LoginModule",
				AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
				options) };
	}

}