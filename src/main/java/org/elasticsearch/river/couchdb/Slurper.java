package org.elasticsearch.river.couchdb;

import static org.elasticsearch.common.base.Joiner.on;
import static org.elasticsearch.common.base.Throwables.propagate;
import static org.elasticsearch.river.couchdb.util.Helpers.bufferedUtf8ReaderFor;
import static org.elasticsearch.river.couchdb.util.Helpers.closeQuietly;
import static org.elasticsearch.river.couchdb.util.Sleeper.sleepLong;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

public class Slurper implements Runnable {

    private final ESLogger logger;

    private final CouchdbConnectionConfig connectionConfig;
    private final CouchdbDatabaseConfig databaseConfig;
    private final LastSeqReader lastSeqReader;
    private final BlockingQueue<String> stream;

    private final UrlBuilder changesFeedUrlBuilder;
    private volatile boolean closed;

    public Slurper(CouchdbConnectionConfig connectionConfig, CouchdbDatabaseConfig databaseConfig,
                   LastSeqReader lastSeqReader, BlockingQueue<String> stream) {
        this.connectionConfig = connectionConfig;
        this.databaseConfig = databaseConfig;
        this.lastSeqReader = lastSeqReader;
        this.stream = stream;

        logger = Loggers.getLogger(Slurper.class, name());

        changesFeedUrlBuilder = new UrlBuilder(connectionConfig, databaseConfig);
    }

    private String name() {
        return on(":").join(getClass().getSimpleName(), databaseConfig.getDatabase());
    }

    @Override
    public void run() {
        while (!closed) {
            try {
                slurp();
            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                logger.warn("Slurper error for database=[{}].", e, databaseConfig.getDatabase());
                sleepLong("to avoid log flooding");
            }
        }
        logger.info("Closing " + name());
    }

    private void slurp() throws InterruptedException {
        Optional<String> lastSeq = lastSeqReader.readLastSequenceFromIndex();
        changesFeedUrlBuilder.withLastSeq(lastSeq);

        HttpURLConnection connection = null;
        BufferedReader reader = null;
        try {
            URL url = changesFeedUrlBuilder.build();
            logger.debug("Will use changes' feed URL=[{}] for database=[{}].", url, databaseConfig.getDatabase());

            connection = configureConnection(url);

            boolean successfullyConnected = connect(connection);
            InputStream is = successfullyConnected ? connection.getInputStream() : connection.getErrorStream();
            reader = bufferedUtf8ReaderFor(is);

            blockingReadFromConnection(reader);

        } catch (InterruptedException ie) {
            throw ie;
        } catch (Exception e) {
            logger.warn("Error occurred when polling for CouchDb changes for database=[{}].", databaseConfig.getDatabase());
            throw propagate(e);
        } finally {
            closeQuietly(connection, reader);
        }
    }

    private HttpURLConnection configureConnection(URL url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        if (connectionConfig.requiresAuthentication()) {
            connection.addRequestProperty("Authorization", connectionConfig.getBasicAuthHeader());
        }

        connection.setDoInput(true);
        connection.setUseCaches(false);

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

    private boolean connect(HttpURLConnection connection) throws IOException {
        connection.connect();
        return connection.getResponseCode() / 100 == 2;
    }

    private void blockingReadFromConnection(BufferedReader reader) throws IOException, InterruptedException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
                logger.trace("Received a heartbeat from CouchDB.");
                continue;
            }
            logger.trace("Received an update=[{}].", line);

            stream.put(line);
        }
    }

    public void close() {
        closed = true;
    }
}