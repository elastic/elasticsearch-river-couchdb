package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.common.base.Optional.fromNullable;
import static org.elasticsearch.river.couchdb.kernel.shared.Constants.LAST_SEQ;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.indexerLogger;
import static org.elasticsearch.river.couchdb.util.Sleeper.sleepLong;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.annotations.VisibleForTesting;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.logging.ESLogger;

public class Indexer implements Runnable {

    private final ESLogger logger;

    private final ChangeCollector changeCollector;
    private final Client client;
    private final LastSeqFormatter lastSeqFormatter;
    private final RequestFactory requestFactory;

    private volatile boolean closed;

    public Indexer(String database, ChangeCollector changeCollector, Client client, LastSeqFormatter lastSeqFormatter,
                   RequestFactory requestFactory) {
        this.changeCollector = changeCollector;
        this.client = client;
        this.lastSeqFormatter = lastSeqFormatter;
        this.requestFactory = requestFactory;

        logger = indexerLogger(Indexer.class, database);
    }

    @Override
    public void run() {
        while (!closed) {
            try {
                Optional<String> indexedSeq = index();
                if (indexedSeq.isPresent()) {
                    logger.debug("Succeeded to index change with seq=[{}].", indexedSeq.get());
                }
            } catch (InterruptedException ie) {
                close();
            } catch (BulkRequestException bre) {
                logger.warn("Failed to execute bulk request.", bre);
            } catch (Exception e) {
                logger.error("Unhandled error.", e);
                sleepLong("to avoid log flooding");
            }
        }
        logger.info("Closed.");
    }

    @VisibleForTesting
    Optional<String> index() throws InterruptedException {
        BulkRequestBuilder bulk = client.prepareBulk();

        Object rawLastSeq = changeCollector.collectAndProcessChanges(bulk);
        String lastSeq = lastSeqFormatter.format(rawLastSeq);

        if (lastSeq != null) {
            bulk.add(requestFactory.aRequestToUpdateLastSeq(lastSeq));
            logger.debug("Will update {} to [{}].", LAST_SEQ, lastSeq);
        }

        if (bulk.numberOfActions() > 0) {
            executeBulkRequest(bulk);
        }
        return fromNullable(lastSeq);
    }

    private void executeBulkRequest(BulkRequestBuilder bulk) {
        try {
            BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
                throw new BulkRequestException(response.buildFailureMessage());
            }
        } catch (ElasticSearchException ese) {
            throw new BulkRequestException(ese);
        }
    }

    public void close() {
        closed = true;
    }
}

