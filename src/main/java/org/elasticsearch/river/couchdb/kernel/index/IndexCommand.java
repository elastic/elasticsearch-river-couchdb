package org.elasticsearch.river.couchdb.kernel.index;

import org.elasticsearch.action.bulk.BulkRequestBuilder;

public class IndexCommand {
    private final BulkRequestBuilder bulk;
    private final String lastSeq;

    public IndexCommand(BulkRequestBuilder bulk, String lastSeq) {
        this.bulk = bulk;
        this.lastSeq = lastSeq;
    }

    public BulkRequestBuilder getBulk() {
        return bulk;
    }

    public String getLastSeq() {
        return lastSeq;
    }
}
