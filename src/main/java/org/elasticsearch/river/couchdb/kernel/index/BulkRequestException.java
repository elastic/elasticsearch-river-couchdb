package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.river.couchdb.util.Helpers.nullToEmpty;
import org.elasticsearch.index.mapper.MapperParsingException;

public class BulkRequestException extends RuntimeException {

    public BulkRequestException(String msg) {
        super(msg);
    }

    public BulkRequestException(Throwable cause) {
        super(cause);
    }

    public boolean isRecoverable() {
        boolean wasCausedByMapperParsingException = nullToEmpty(getMessage())
                .contains(MapperParsingException.class.getName());

        return !wasCausedByMapperParsingException;
    }
}
