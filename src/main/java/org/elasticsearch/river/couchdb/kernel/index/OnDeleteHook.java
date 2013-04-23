package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.river.couchdb.util.LoggerHelper.indexerLogger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.common.logging.ESLogger;
import java.util.Map;

public abstract class OnDeleteHook {

    protected final ESLogger logger;

    protected OnDeleteHook(String database) {
        logger = indexerLogger(getClass(), database);
    }

    public abstract DeleteRequest onDelete(Map<String, Object> document);

}
