package org.elasticsearch.river.couchdb.kernel.index;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.common.base.Optional.fromNullable;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.indexerLogger;
import static org.elasticsearch.river.couchdb.util.Sleeper.sleepLong;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.river.couchdb.IndexConfig;
import java.util.concurrent.BlockingQueue;

public class Indexer implements Runnable {

    public static final String LAST_SEQ = "last_seq";

    private final ESLogger logger;

    private final BlockingQueue<String> changesStream;
    private final Client client;
    private final LastSeqFormatter lastSeqFormatter;
    private final ChangeProcessor changeProcessor;
    private final RequestFactory requestFactory;

    private final IndexConfig indexConfig;

    private volatile boolean closed;

    public Indexer(String database, BlockingQueue<String> stream, Client client, LastSeqFormatter lastSeqFormatter,
                   ChangeProcessor changeProcessor, RequestFactory requestFactory, IndexConfig indexConfig) {
        this.changesStream = stream;
        this.client = client;
        this.lastSeqFormatter = lastSeqFormatter;
        this.changeProcessor = changeProcessor;
        this.requestFactory = requestFactory;
        this.indexConfig = indexConfig;

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

    private Optional<String> index() throws InterruptedException {
        BulkRequestBuilder bulk = client.prepareBulk();

        Object rawLastSeq = processChanges(bulk);
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

    @Nullable
    private Object processChanges(BulkRequestBuilder bulk) throws InterruptedException {
        String change = changesStream.take();

        Object lineSeq = changeProcessor.processChange(change, bulk);
        Object lastSeq = lineSeq;

        // spin a bit to see if we can get some more changes
        while ((change = changesStream.poll(indexConfig.getBulkTimeout().millis(), MILLISECONDS)) != null) {
            lineSeq = changeProcessor.processChange(change, bulk);
            if (lineSeq != null) {
                lastSeq = lineSeq;
            }

            if (bulk.numberOfActions() >= indexConfig.getBulkSize()) {
                break;
            }
        }
        return lastSeq;
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

