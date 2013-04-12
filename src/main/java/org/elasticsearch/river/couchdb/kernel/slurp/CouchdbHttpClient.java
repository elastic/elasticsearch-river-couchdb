package org.elasticsearch.river.couchdb.kernel.slurp;

import static org.elasticsearch.common.base.Throwables.propagate;
import static org.elasticsearch.river.couchdb.util.Helpers.bufferedUtf8ReaderFor;
import static org.elasticsearch.river.couchdb.util.Helpers.closeQuietly;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.slurperLogger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.river.couchdb.CouchdbConnectionConfig;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class CouchdbHttpClient {

    private final ESLogger logger;

    private final CouchdbConnectionConfig connectionConfig;
    private final ChangeHandler changeHandler;

    public CouchdbHttpClient(String database, CouchdbConnectionConfig connectionConfig, ChangeHandler changeHandler) {
        this.connectionConfig = connectionConfig;
        this.changeHandler = changeHandler;

        logger = slurperLogger(CouchdbHttpClient.class, database);
    }

    public void listenForChanges(URL changesFeedUrl) throws InterruptedException {
        HttpURLConnection connection = null;
        BufferedReader reader = null;
        try {
            connection = configureConnection(changesFeedUrl);

            reader = bufferedUtf8ReaderFor(connectAndExtractStream(connection));

            blockingReadFromConnection(reader);

        } catch (InterruptedException ie) {
            throw ie;
        } catch (Exception e) {
            logger.warn("Error occurred when listening for CouchDB changes.");
            propagate(e);
        } finally {
            closeQuietly(connection, reader);
        }
    }

    private HttpURLConnection configureConnection(URL url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoInput(true);
        connection.setUseCaches(false);

        if (connectionConfig.requiresAuthentication()) {
            connection.addRequestProperty("Authorization", connectionConfig.getBasicAuthHeader());
        }

        if (!connectionConfig.shouldVerifyHostname()) {
            ((HttpsURLConnection) connection).setHostnameVerifier(
                    new HostnameVerifier() {
                        public boolean verify(String string, SSLSession ssls) {
                            return true;
                        }
                    }
            );
        }
        return connection;
    }

    private InputStream connectAndExtractStream(HttpURLConnection connection) throws IOException {
        connection.connect();

        boolean successfullyConnected = connection.getResponseCode() / 100 == 2;
        return successfullyConnected ? connection.getInputStream() : connection.getErrorStream();
    }

    private void blockingReadFromConnection(BufferedReader reader) throws IOException, InterruptedException {
        String line;
        while ((line = reader.readLine()) != null) {
            changeHandler.handleChange(line);
        }
    }
}
