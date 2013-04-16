package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.river.couchdb.util.LoggerHelper.indexerLogger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import java.util.Map;

public abstract class OnIndexHook {

    protected final ESLogger logger;

    protected OnIndexHook(String database) {
        logger = indexerLogger(getClass(), database);
    }

    public IndexRequest onIndex(Map<String, Object> document) {
        if (!shouldBeIndexed(document)) {
            return null;
        }

        return doIndex(document);
    }

    protected abstract boolean shouldBeIndexed(Map<String, Object> document);

    protected abstract IndexRequest doIndex(Map<String, Object> document);
}
