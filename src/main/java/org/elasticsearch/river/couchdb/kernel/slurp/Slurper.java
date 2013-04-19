package org.elasticsearch.river.couchdb.kernel.slurp;

import static org.elasticsearch.river.couchdb.kernel.shared.Constants.LAST_SEQ;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.slurperLogger;
import static org.elasticsearch.river.couchdb.util.Sleeper.sleepLong;
import org.elasticsearch.common.annotations.VisibleForTesting;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.logging.ESLogger;
import java.net.URL;

public class Slurper implements Runnable {

    private final ESLogger logger;

    private final LastSeqReader lastSeqReader;
    private final UrlBuilder changesFeedUrlBuilder;
    private final CouchdbHttpClient couchdbHttpClient;

    private volatile boolean closed;

    public Slurper(String database, LastSeqReader lastSeqReader, UrlBuilder changesFeedUrlBuilder,
                   CouchdbHttpClient couchdbHttpClient) {
        this.lastSeqReader = lastSeqReader;
        this.changesFeedUrlBuilder = changesFeedUrlBuilder;
        this.couchdbHttpClient = couchdbHttpClient;

        logger = slurperLogger(Slurper.class, database);
    }

    @Override
    public void run() {
        while (!closed) {
            try {
                slurp();
            } catch (InterruptedException ie) {
                close();
            } catch (Exception e) {
                logger.warn("Unhandled error, throttling.", e);
                sleepLong("to avoid log flooding");
            }
        }
        logger.info("Closed.");
    }

    @VisibleForTesting
    void slurp() throws InterruptedException {
        Optional<String> lastSeq = lastSeqReader.readLastSequenceFromIndex();
        logger.info("Read {}=[{}] from index.", LAST_SEQ, lastSeq);
        changesFeedUrlBuilder.withLastSeq(lastSeq);

        URL changesFeedUrl = changesFeedUrlBuilder.build();
        logger.debug("Will use changes' feed URL=[{}].", changesFeedUrl);

        couchdbHttpClient.listenForChanges(changesFeedUrl);
    }

    public void close() {
        closed = true;
    }
}