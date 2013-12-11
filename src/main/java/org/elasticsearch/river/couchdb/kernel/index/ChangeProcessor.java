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
    private final IndexConfig indexConfig;
    private final OnIndexHook onIndexHook;
    private final OnDeleteHook onDeleteHook;

    public ChangeProcessor(String database, ExecutableScript script, IndexConfig indexConfig,
                           OnIndexHook onIndexHook, OnDeleteHook onDeleteHook) {
        this.script = script;
        this.indexConfig = indexConfig;
        this.onIndexHook = onIndexHook;
        this.onDeleteHook = onDeleteHook;

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

        id = (ctx.get("id") == null) ? null : ctx.get("id").toString();

        if (Boolean.TRUE.equals(ctx.get("ignore"))) {
            logger.debug("Ignoring update of document [id={}]; CouchDB changes feed seq=[{}].", id, seq);
        } else if (Boolean.TRUE.equals(ctx.get("deleted"))) {
            logger.debug("Processing document [id={}] marked as \"deleted\"; CouchDB changes feed seq=[{}].", id, seq);
            bulk.add(onDeleteHook.onDelete(doc));
        } else if (doc != null) {
            doc.remove("_rev");

            if (indexConfig.shouldIgnoreAttachments()) {
                doc.remove("_attachments");
            } else {
                // TODO CouchDB river does not really store attachments but only its meta-information
            }

            logger.debug("Processing indexing of document [id={}]; CouchDB changes feed seq=[{}].", id, seq);
            bulk.add(onIndexHook.onIndex(doc));
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
}
