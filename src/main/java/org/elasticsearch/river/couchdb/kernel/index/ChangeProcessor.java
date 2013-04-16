package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.common.xcontent.XContentFactory.xContent;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.indexerLogger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.river.couchdb.IndexConfig;
import org.elasticsearch.script.ExecutableScript;
import java.io.IOException;
import java.util.Map;

public class ChangeProcessor {

    private final ESLogger logger;

    private final ExecutableScript script;
    private final RequestFactory requestFactory;
    private final IndexConfig indexConfig;

    public ChangeProcessor(String database, ExecutableScript script, RequestFactory requestFactory,
                           IndexConfig indexConfig) {
        this.script = script;
        this.requestFactory = requestFactory;
        this.indexConfig = indexConfig;

        logger = indexerLogger(ChangeProcessor.class, database);
    }

    @Nullable
    public Object processChange(String change, BulkRequestBuilder bulk) {
        Map<String, Object> ctx = parseAndValidateChange(change);
        if (ctx == null) {
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
        } catch (RuntimeException e) {
            logger.warn("Failed to run script on context=[{}].", e, ctx);
            return seq;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> doc = (Map<String, Object>) ctx.get("doc");

        String index = extractIndex(ctx);
        String type = extractType(ctx);
        String routing = extractRouting(ctx);
        String parent = extractParent(ctx);

        if (Boolean.TRUE.equals(ctx.get("ignore"))) {
            logger.debug("Ignoring update of document [id={}]; CouchDB changes feed seq=[{}].", id, seq);
        } else if (Boolean.TRUE.equals(ctx.get("deleted"))) {
            logger.debug("Processing document [id={}] marked as \"deleted\"; CouchDB changes feed seq=[{}].", id, seq);

            bulk.add(requestFactory.aDeleteRequest(index, type, id, routing, parent));
        } else if (doc != null) {
            doc.remove("_rev");

            if (indexConfig.shouldIgnoreAttachments()) {
                doc.remove("_attachments");
            } else {
                // TODO CouchDB river does not really store attachments but only its meta-information
            }

            logger.trace("Processing document=[{}]; CouchDB changes feed seq=[{}].", doc, seq);

            bulk.add(requestFactory.anIndexRequest(index, type, id, doc, routing, parent));
        } else {
            logger.warn("Ignoring unknown change=[{}]; CouchDB changes feed seq=[{}].", change, seq);
        }
        return seq;
    }

    private Map<String, Object> parseAndValidateChange(String change) {
        Map<String, Object> ctx;
        try {
            ctx = xContent(JSON).createParser(change).mapAndClose();
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
        return ctx;
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

    private String extractIndex(Map<String, Object> ctx) {
        String index = (String) ctx.get("_index");
        if (index == null) {
            index = indexConfig.getName();
        }
        return index;
    }

    private String extractType(Map<String, Object> ctx) {
        String type = (String) ctx.get("_type");
        if (type == null) {
            type = indexConfig.getType();
        }
        return type;
    }

    private String extractRouting(Map<String, Object> ctx) {
        return (String) ctx.get("_routing");
    }

    private String extractParent(Map<String, Object> ctx) {
        return (String) ctx.get("_parent");
    }
}
