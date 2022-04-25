package io.firebolt.jdbc.client.ssl;

import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

/**
 * A {@link javax.net.ssl.TrustManager} that doesn't do any validation
 */
public class InsecureTrustManager implements X509TrustManager {

    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    @Override
    public void checkClientTrusted(X509Certificate[] certs, String authType) {
        //This must be empty as we don't need any check
    }

    @Override
    public void checkServerTrusted(X509Certificate[] certs, String authType) {
        //This must be empty as we don't need any check
    }
}
