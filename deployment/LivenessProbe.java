import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Enumeration;

public final class LivenessProbe {
    private static final URI LIVENESS_URI =
        URI.create("https://127.0.0.1:443/api/health/liveness");

    private LivenessProbe() {
    }

    public static void main(String[] arguments) throws Exception {
        String username = requiredEnvironmentVariable("BOT_BASIC_USERNAME");
        String password = requiredEnvironmentVariable("BOT_BASIC_PASSWORD");
        String keyStorePath = requiredEnvironmentVariable("TLS_KEYSTORE_PATH");
        String keyStorePassword =
            requiredEnvironmentVariable("TLS_KEYSTORE_PASSWORD");

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(Path.of(keyStorePath))) {
            keyStore.load(input, keyStorePassword.toCharArray());
        }
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        int certificateIndex = 0;
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            Certificate[] chain = keyStore.getCertificateChain(alias);
            if (chain != null) {
                for (Certificate certificate : chain) {
                    trustStore.setCertificateEntry(
                        "certificate-" + certificateIndex++,
                        certificate
                    );
                }
            } else {
                Certificate certificate = keyStore.getCertificate(alias);
                if (certificate != null) {
                    trustStore.setCertificateEntry(
                        "certificate-" + certificateIndex++,
                        certificate
                    );
                }
            }
        }
        if (certificateIndex == 0) {
            throw new IllegalStateException("TLS keystore contains no certificate");
        }
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
            TrustManagerFactory.getDefaultAlgorithm()
        );
        trustManagerFactory.init(trustStore);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);

        HttpsURLConnection connection =
            (HttpsURLConnection) LIVENESS_URI.toURL().openConnection();
        connection.setSSLSocketFactory(sslContext.getSocketFactory());
        HostnameVerifier loopbackVerifier = (host, session) ->
            "127.0.0.1".equals(host);
        connection.setHostnameVerifier(loopbackVerifier);
        connection.setConnectTimeout(3_000);
        connection.setReadTimeout(3_000);
        connection.setRequestMethod("GET");
        connection.setRequestProperty(
            "Authorization",
            "Basic " + Base64.getEncoder().encodeToString(
                (username + ":" + password).getBytes(StandardCharsets.UTF_8)
            )
        );
        connection.connect();
        int responseCode = connection.getResponseCode();
        connection.disconnect();
        if (responseCode != 200) {
            throw new IllegalStateException(
                "HTTPS liveness returned status " + responseCode
            );
        }
    }

    private static String requiredEnvironmentVariable(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required");
        }
        return value;
    }
}
