package org.elasticsearch.river.couchdb.kernel.index;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.base.Optional.fromNullable;
import static org.elasticsearch.common.base.Throwables.propagate;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.xContent;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.indexerLogger;
import static org.elasticsearch.river.couchdb.util.Sleeper.sleepLong;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.river.couchdb.IndexConfig;
import org.elasticsearch.river.couchdb.RiverConfig;
import org.elasticsearch.script.ExecutableScript;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class Indexer implements Runnable {

    public static final String LAST_SEQ = "last_seq";

    private final ESLogger logger;

    private final String database;
    private final BlockingQueue<String> changesStream;
    private final Client client;
    private final LastSeqFormatter lastSeqFormatter;

    private ExecutableScript script;
    private final IndexConfig indexConfig;
    private final RiverConfig riverConfig;

    private volatile boolean closed;

    public Indexer(String database, BlockingQueue<String> stream, Client client, LastSeqFormatter lastSeqFormatter,
                   ExecutableScript script, IndexConfig indexConfig,
                   RiverConfig riverConfig) {
        this.database = database;
        this.changesStream = stream;
        this.client = client;
        this.lastSeqFormatter = lastSeqFormatter;
        this.script = script;
        this.indexConfig = indexConfig;
        this.riverConfig = riverConfig;

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
            bulk.add(aRequestToUpdateLastSeq(lastSeq));
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

    @Nullable
    private Object processChanges(BulkRequestBuilder bulk) throws InterruptedException {
        String change = changesStream.take();

        Object lineSeq = processChange(change, bulk);
        Object lastSeq = lineSeq;

        // spin a bit to see if we can get some more changes
        while ((change = changesStream.poll(indexConfig.getBulkTimeout().millis(), MILLISECONDS)) != null) {
            lineSeq = processChange(change, bulk);
            if (lineSeq != null) {
                lastSeq = lineSeq;
            }

            if (bulk.numberOfActions() >= indexConfig.getBulkSize()) {
                break;
            }
        }
        return lastSeq;
    }

    private IndexRequest aRequestToUpdateLastSeq(String lastSeq) {
        logger.debug("Will update {} to [{}].", LAST_SEQ, lastSeq);

        return indexRequest(riverConfig.getRiverIndexName())
                .type(riverConfig.getRiverName().name())
                .id("_seq")
                .source(lastSeqSource(lastSeq));
    }

    private XContentBuilder lastSeqSource(String lastSeq) {
        try {
            return jsonBuilder().startObject()
                    .startObject(database).field(LAST_SEQ, lastSeq).endObject()
                    .endObject();
        } catch (IOException ioe) {
            logger.error("Could not build a valid JSON to carry information about {}.", LAST_SEQ);
            throw propagate(ioe);
        }
    }

    @Nullable
    private Object processChange(String change, BulkRequestBuilder bulk) {
        Map<String, Object> ctx;
        try {
            ctx = xContent(XContentType.JSON).createParser(change).mapAndClose();
        } catch (IOException e) {
            logger.warn("Failed to parse change=[{}].", e, change);
            return null;
        }
        if (ctx.containsKey("error")) {
            logger.warn("Error=[{}] when processing change=[{}], reason=[{}].",
                    ctx.get("error"), change, ctx.get("reason"));
            return null;
        }
        if (!ctx.containsKey("id") || !ctx.containsKey("seq")) {
            logger.warn("Missing id or seq in change=[{}].", change);
            return null;
        }

        String seq = ctx.get("seq").toString();
        String id = ctx.get("id").toString();

        if (id.startsWith("_design/")) {
            logger.trace("Ignoring design document with id=[{}].", id);
            return seq;
        }

        try {
            runScriptIfProvided(ctx);
        } catch (Exception e) {
            logger.warn("Failed to run script on context=[{}].", e, ctx);
            return seq;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> doc = (Map<String, Object>) ctx.get("doc");

        if (Boolean.TRUE.equals(ctx.get("ignore"))) {
            logger.debug("Ignoring update of document [id={}]; CouchDB changes feed seq=[{}].", id, seq);
        } else if (Boolean.TRUE.equals(ctx.get("deleted"))) {
            logger.debug("Processing document [id={}] marked as \"deleted\"; CouchDB changes feed seq=[{}].", id, seq);

            String index = extractIndex(ctx);
            String type = extractType(ctx);
            bulk.add(deleteRequest(index).type(type).id(id).routing(extractRouting(ctx)).parent(extractParent(ctx)));
        } else if (doc != null) {
            doc.remove("_rev");

            if (indexConfig.shouldIgnoreAttachments()) {
                doc.remove("_attachments");
            } else {
                // TODO CouchDB river does not really store attachments but only its meta-information
            }

            logger.trace("Processing document=[{}]; CouchDB changes feed seq=[{}].", doc, seq);

            String index = extractIndex(ctx);
            String type = extractType(ctx);
            bulk.add(indexRequest(index).type(type).id(id).source(doc).routing(extractRouting(ctx)).parent(extractParent(ctx)));
        } else {
            logger.warn("Ignoring unknown change=[{}]; CouchDB changes feed seq=[{}].", change, seq);
        }
        return seq;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> runScriptIfProvided(Map<String, Object> ctx) {
        if (script == null) {
            return ctx;
        } else {
            script.setNextVar("ctx", ctx);
            script.run();
            return (Map<String, Object>) script.unwrap(ctx);
        }
    }

    private String extractParent(Map<String, Object> ctx) {
        return (String) ctx.get("_parent");
    }

    private String extractRouting(Map<String, Object> ctx) {
        return (String) ctx.get("_routing");
    }

    private String extractType(Map<String, Object> ctx) {
        String type = (String) ctx.get("_type");
        if (type == null) {
            type = indexConfig.getType();
        }
        return type;
    }

    private String extractIndex(Map<String, Object> ctx) {
        String index = (String) ctx.get("_index");
        if (index == null) {
            index = indexConfig.getName();
        }
        return index;
    }

    public void close() {
        closed = true;
    }
}

