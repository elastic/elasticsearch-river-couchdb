package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.base.Throwables.propagate;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.river.couchdb.kernel.shared.Constants.LAST_SEQ;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.indexerLogger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.river.couchdb.RiverConfig;
import java.io.IOException;
import java.util.Map;

public class RequestFactory {

    private final ESLogger logger;

    private final String database;
    private final RiverConfig riverConfig;

    public RequestFactory(String database, RiverConfig riverConfig) {
        this.database = database;
        this.riverConfig = riverConfig;

        logger = indexerLogger(RequestFactory.class, database);
    }

    public IndexRequest aRequestToUpdateLastSeq(String lastSeq) {
        return indexRequest(riverConfig.getRiverIndexName())
                .type(riverConfig.getRiverName().name())
                .id(LAST_SEQ)
                .source(lastSeqSource(lastSeq));
    }

    private XContentBuilder lastSeqSource(String lastSeq) {
        try {
            return jsonBuilder().startObject()
                    .startObject(database).field(LAST_SEQ, lastSeq).endObject()
                    .endObject();
        } catch (IOException ioe) {
            logger.error("Could not build a valid JSON to carry information about {}.", LAST_SEQ);
            throw propagate(ioe);
        }
    }

    public DeleteRequest aDeleteRequest(String index, String type, String id, String routing, String parent) {
        return deleteRequest(index).type(type).id(id).routing(routing).parent(parent);
    }

    public IndexRequest anIndexRequest(String index, String type, String id, Map<String, Object> doc,
                                       String routing, String parent) {
        return indexRequest(index).type(type).id(id).source(doc).routing(routing).parent(parent);
    }

}
