package org.elasticsearch.river.couchdb.kernel.index;

public class BulkRequestException extends RuntimeException {

    public BulkRequestException(String msg) {
        super(msg);
    }

    public BulkRequestException(Throwable cause) {
        super(cause);
    }
}
