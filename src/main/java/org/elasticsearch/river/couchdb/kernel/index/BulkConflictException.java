package org.elasticsearch.river.couchdb.kernel.index;

public class BulkConflictException extends RuntimeException {

    public BulkConflictException(String msg) {
        super(msg);
    }

    public BulkConflictException(Throwable cause) {
        super(cause);
    }
}
