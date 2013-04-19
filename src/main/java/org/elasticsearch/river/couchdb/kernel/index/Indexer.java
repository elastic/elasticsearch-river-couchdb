package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.common.base.Optional.fromNullable;
import static org.elasticsearch.river.couchdb.kernel.shared.Constants.LAST_SEQ;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.indexerLogger;
import static org.elasticsearch.river.couchdb.util.Sleeper.sleep;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.annotations.VisibleForTesting;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.river.couchdb.kernel.shared.ClientWrapper;

public class Indexer implements Runnable {

    private final ESLogger logger;

    private final ChangeCollector changeCollector;
    private final ClientWrapper clientWrapper;
    private final LastSeqFormatter lastSeqFormatter;
    private final RequestFactory requestFactory;

    private volatile boolean closed;

    public Indexer(String database, ChangeCollector changeCollector, ClientWrapper clientWrapper,
                   LastSeqFormatter lastSeqFormatter, RequestFactory requestFactory) {
        this.changeCollector = changeCollector;
        this.clientWrapper = clientWrapper;
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
                sleep("to avoid log flooding");
            }
        }
        logger.info("Closed.");
    }

    @VisibleForTesting
    Optional<String> index() throws InterruptedException {
        BulkRequestBuilder bulk = clientWrapper.prepareBulkRequest();

        Object rawLastSeq = changeCollector.collectAndProcessChanges(bulk);
        String lastSeq = lastSeqFormatter.format(rawLastSeq);
        logger.debug("Received and processed a change with {}=[{}]", LAST_SEQ, lastSeq);

        if (lastSeq != null) {
            bulk.add(requestFactory.aRequestToUpdateLastSeq(lastSeq));
            logger.info("Will update {} to [{}].", LAST_SEQ, lastSeq);
        }

        if (bulk.numberOfActions() > 0) {
            executeBulkRequest(bulk);
        }
        return fromNullable(lastSeq);
    }

    private void executeBulkRequest(BulkRequestBuilder bulk) {
        try {
            BulkResponse response = clientWrapper.executeBulkRequest(bulk);
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

