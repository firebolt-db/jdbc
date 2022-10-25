package com.firebolt.jdbc.client.authentication;

import org.apache.commons.lang3.StringUtils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class AuthenticationRequestFactory {

	public static AuthenticationRequest getAuthenticationRequest(String username, String password, String host) {
		if (StringUtils.isEmpty(username) || StringUtils.contains(username, "@")) {
			return new UsernamePasswordAuthenticationRequest(username, password, host);
		} else {
			return new ServiceAccountAuthenticationRequest(username, password, host);
		}
	}

}
