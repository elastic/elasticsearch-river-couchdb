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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.river.couchdb.kernel.shared.ClientWrapper;

public class Indexer implements Runnable {

    private final ESLogger logger;

    private final ChangeCollector changeCollector;
    private final ClientWrapper clientWrapper;
    private final LastSeqFormatter lastSeqFormatter;
    private final RequestFactory requestFactory;
    private final RetryHandler<IndexCommand> retryHandler;

    private volatile boolean closed;

    public Indexer(String database, ChangeCollector changeCollector, ClientWrapper clientWrapper,
                   LastSeqFormatter lastSeqFormatter, RequestFactory requestFactory,
                   RetryHandler<IndexCommand> retryHandler) {
        this.changeCollector = changeCollector;
        this.clientWrapper = clientWrapper;
        this.lastSeqFormatter = lastSeqFormatter;
        this.requestFactory = requestFactory;
        this.retryHandler = retryHandler;

        logger = indexerLogger(Indexer.class, database);
    }

    @Override
    public void run() {
        while (!closed) {
            singleIteration();
        }
        logger.info("Closed.");
    }

    @VisibleForTesting
    void singleIteration() {
        try {
            Optional<String> indexedSeq = index();
            if (indexedSeq.isPresent()) {
                logger.debug("Succeeded to index change with seq=[{}].", indexedSeq.get());
            }
            retryHandler.doNotRetry();
        } catch (InterruptedException ie) {
            close();
        } catch (BulkConflictException bce) {
            logger.debug("Index conflict while executing bulk request. Will retry.", bce);
            sleep("to avoid log flooding");
        } catch (BulkRequestException bre) {
            if (bre.isRecoverable()) {
                logger.info("Failed to execute bulk request. Will retry.", bre);
            } else {
                logger.error("Failed to execute bulk request. Will not retry.", bre);
                retryHandler.doNotRetry();
            }
        } catch (Exception e) {
            retryHandler.doNotRetry();
            logger.error("Unhandled error.", e);
            sleep("to avoid log flooding");
        }
    }

    @VisibleForTesting
    Optional<String> index() throws InterruptedException {
        IndexCommand cmd;
        if (retryHandler.shouldRetryLastAttempt()) {
            cmd = retryHandler.getLastCommand();
        } else {
            cmd = prepareANewIndexOperation();
            retryHandler.newCommand(cmd);
        }

        if (cmd.getBulk().numberOfActions() > 0) {
            executeBulkRequest(cmd.getBulk());
        }
        return fromNullable(cmd.getLastSeq());
    }

    private IndexCommand prepareANewIndexOperation() throws InterruptedException {
        BulkRequestBuilder bulk = clientWrapper.prepareBulkRequest();

        Object rawLastSeq = changeCollector.collectAndProcessChanges(bulk);
        String lastSeq = lastSeqFormatter.format(rawLastSeq);
        logger.debug("Received and processed a change with {}=[{}]", LAST_SEQ, lastSeq);

        if (lastSeq != null) {
            bulk.add(requestFactory.aRequestToUpdateLastSeq(lastSeq));
            logger.info("Will update {} to [{}].", LAST_SEQ, lastSeq);
        }
        return new IndexCommand(bulk, lastSeq);
    }

    private void executeBulkRequest(BulkRequestBuilder bulk) {
        try {
            BulkResponse response = clientWrapper.executeBulkRequest(bulk);
            if (response.hasFailures()) {
                throw new BulkRequestException(response.buildFailureMessage());
            }
        } catch (ElasticSearchException ese) {
            if (ese.status() == RestStatus.CONFLICT) {
                throw new BulkConflictException(ese);
            } else {
                throw new BulkRequestException(ese);
            }
        }
    }

    public void close() {
        closed = true;
    }
}

