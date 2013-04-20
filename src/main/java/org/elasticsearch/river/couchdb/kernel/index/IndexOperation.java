package org.elasticsearch.river.couchdb.kernel.index;

import org.elasticsearch.action.bulk.BulkRequestBuilder;

public class IndexOperation {
    private final BulkRequestBuilder bulk;
    private final String lastSeq;

    public IndexOperation(BulkRequestBuilder bulk, String lastSeq) {
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
