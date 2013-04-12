package org.elasticsearch.river.couchdb.kernel.slurp;

import static org.elasticsearch.river.couchdb.util.LoggerHelper.slurperLogger;
import org.elasticsearch.common.logging.ESLogger;
import java.util.concurrent.BlockingQueue;

public class ChangeHandler {

    private final ESLogger logger;
    private final BlockingQueue<String> changesQueue;

    public ChangeHandler(String database, BlockingQueue<String> changesQueue) {
        this.changesQueue = changesQueue;

        this.logger = slurperLogger(ChangeHandler.class, database);
    }

    public void handleChange(String change) throws InterruptedException {
        if (change.isEmpty()) {
            logger.trace("Received a heartbeat from CouchDB.");
            return;
        }
        logger.trace("Received an update=[{}].", change);

        changesQueue.put(change);
    }
}
