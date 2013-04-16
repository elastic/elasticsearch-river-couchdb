package org.elasticsearch.river.couchdb.kernel.index;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.river.couchdb.IndexConfig;
import java.util.concurrent.BlockingQueue;

public class ChangeCollector {

    private final BlockingQueue<String> changesStream;
    private final ChangeProcessor changeProcessor;
    private final IndexConfig indexConfig;

    public ChangeCollector(BlockingQueue<String> changesStream, IndexConfig indexConfig,
                           ChangeProcessor changeProcessor) {
        this.changesStream = changesStream;
        this.indexConfig = indexConfig;
        this.changeProcessor = changeProcessor;
    }

    @Nullable
    public Object collectAndProcessChanges(BulkRequestBuilder bulk) throws InterruptedException {
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
}
