package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.common.base.Preconditions.checkNotNull;
import static org.elasticsearch.common.base.Throwables.propagate;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.river.couchdb.kernel.index.Indexer.LAST_SEQ;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.indexerLogger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.io.IOException;
import java.util.List;

public class LastSeqFormatter {

    private final ESLogger logger;

    public LastSeqFormatter(String database) {
        logger = indexerLogger(LastSeqFormatter.class, database);
    }

    public String format(Object lastSeq) {
        checkNotNull(lastSeq);
        if (lastSeq instanceof List) {
            // BigCouch uses array for the seq. @see https://github.com/elasticsearch/elasticsearch/issues/1478
            try {
                XContentBuilder builder = jsonBuilder().startArray();
                for (Object value : ((List) lastSeq)) {
                    builder.value(value);
                }
                builder.endArray();
                return builder.string();
            } catch (IOException e) {
                logger.error("Failed to convert {}=[{}] to JSON. ", LAST_SEQ, lastSeq);
                throw propagate(e);
            }
        } else {
            return lastSeq.toString();
        }
    }
}
