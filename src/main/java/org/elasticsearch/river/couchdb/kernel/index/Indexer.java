package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.base.Throwables.propagate;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.indexerLogger;
import static org.elasticsearch.river.couchdb.util.Sleeper.sleepLong;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.river.couchdb.IndexConfig;
import org.elasticsearch.river.couchdb.RiverConfig;
import org.elasticsearch.script.ExecutableScript;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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
                index();
            } catch (InterruptedException ie) {
                close();
            } catch (Exception e) {
                logger.error("Unhandled error.", e);
                sleepLong("to avoid log flooding");
            }
        }
        logger.info("Closed.");
    }

    private void index() throws InterruptedException {
        String change = changesStream.take();

        BulkRequestBuilder bulk = client.prepareBulk();
        Object lastSeq = null;
        Object lineSeq = processLine(change, bulk);
        if (lineSeq != null) {
            lastSeq = lineSeq;
        }

        // spin a bit to see if we can get some more changes
        while ((change = changesStream.poll(indexConfig.getBulkTimeout().millis(), TimeUnit.MILLISECONDS)) != null) {
            lineSeq = processLine(change, bulk);
            if (lineSeq != null) {
                lastSeq = lineSeq;
            }

            if (bulk.numberOfActions() >= indexConfig.getBulkSize()) {
                break;
            }
        }

        if (lastSeq != null) {
            String lastSeqAsString = lastSeqFormatter.format(lastSeq);

            logger.debug("Will update {} to [{}].", LAST_SEQ, lastSeqAsString);
            bulk.add(aRequestToUpdateLastSeq(lastSeqAsString));
        }

        try {
            BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
                // TODO write to exception queue?
                logger.warn("failed to execute" + response.buildFailureMessage());
            }
        } catch (Exception e) {
            logger.warn("failed to execute bulk", e);
        }
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

    @SuppressWarnings({"unchecked"})
    private Object processLine(String s, BulkRequestBuilder bulk) {
        Map<String, Object> ctx;
        try {
            ctx = XContentFactory.xContent(XContentType.JSON).createParser(s).mapAndClose();
        } catch (IOException e) {
            logger.warn("failed to parse {}", e, s);
            return null;
        }
        if (ctx.containsKey("error")) {
            logger.warn("received error {}", s);
            return null;
        }
        Object seq = ctx.get("seq");
        String id = ctx.get("id").toString();

        // Ignore design documents
        if (id.startsWith("_design/")) {
            if (logger.isTraceEnabled()) {
                logger.trace("ignoring design document {}", id);
            }
            return seq;
        }

        if (script != null) {
            script.setNextVar("ctx", ctx);
            try {
                script.run();
                // we need to unwrap the ctx...
                ctx = (Map<String, Object>) script.unwrap(ctx);
            } catch (Exception e) {
                logger.warn("failed to script process {}, ignoring", e, ctx);
                return seq;
            }
        }

        if (ctx.containsKey("ignore") && ctx.get("ignore").equals(Boolean.TRUE)) {
            // ignore dock
        } else if (ctx.containsKey("deleted") && ctx.get("deleted").equals(Boolean.TRUE)) {
            String index = extractIndex(ctx);
            String type = extractType(ctx);
            if (logger.isTraceEnabled()) {
                logger.trace("processing [delete]: [{}]/[{}]/[{}]", index, type, id);
            }
            bulk.add(deleteRequest(index).type(type).id(id).routing(extractRouting(ctx)).parent(extractParent(ctx)));
        } else if (ctx.containsKey("doc")) {
            String index = extractIndex(ctx);
            String type = extractType(ctx);
            Map<String, Object> doc = (Map<String, Object>) ctx.get("doc");

            // Remove _attachment from doc if needed
            if (indexConfig.shouldIgnoreAttachments()) {
                // no need to log that we removed it, the doc indexed will be shown without it
                doc.remove("_attachments");
            } else {
                // TODO by now, couchDB river does not really store attachments but only attachments meta infomration
                // So we perhaps need to fully support attachments
            }

            if (logger.isTraceEnabled()) {
                logger.trace("processing [index ]: [{}]/[{}]/[{}], source {}", index, type, id, doc);
            }

            bulk.add(indexRequest(index).type(type).id(id).source(doc).routing(extractRouting(ctx)).parent(extractParent(ctx)));
        } else {
            logger.warn("ignoring unknown change {}", s);
        }
        return seq;
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

