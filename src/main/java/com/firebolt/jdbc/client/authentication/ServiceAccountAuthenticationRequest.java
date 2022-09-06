package com.firebolt.jdbc.client.authentication;

import java.util.ArrayList;
import java.util.List;

import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ServiceAccountAuthenticationRequest implements AuthenticationRequest {
	private static final String CLIENT_CREDENTIALS = "client_credentials";
	private static final String GRAND_TYPE_FIELD_NAME = "grant_type";
	private static final String CLIENT_ID_FIELD_NAME = "client_id";
	private static final String CLIENT_SECRET_FIELD_NAME = "client_secret";
	private static final String AUTH_URL = "%s/auth/v1/token";
	private String id;
	private String secret;
	private String host;

	public HttpEntity getHttpEntity() {
		List<NameValuePair> loginInfo = new ArrayList<>();
		loginInfo.add(new BasicNameValuePair(CLIENT_ID_FIELD_NAME, id));
		loginInfo.add(new BasicNameValuePair(CLIENT_SECRET_FIELD_NAME, secret));
		loginInfo.add(new BasicNameValuePair(GRAND_TYPE_FIELD_NAME, CLIENT_CREDENTIALS));
		return new UrlEncodedFormEntity(loginInfo);
	}

	@Override
	public String getUri() {
		return String.format(AUTH_URL, host);
	}
}
