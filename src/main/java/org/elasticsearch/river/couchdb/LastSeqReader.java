package org.elasticsearch.river.couchdb;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import java.util.Map;

public class LastSeqReader {

    public static final String LAST_SEQ = "last_seq";

    private final CouchdbDatabaseConfig databaseConfig;
    private final RiverConfig riverConfig;

    private final Client client;

    public LastSeqReader(CouchdbDatabaseConfig databaseConfig, RiverConfig riverConfig, Client client) {
        this.databaseConfig = databaseConfig;
        this.riverConfig = riverConfig;
        this.client = client;
    }

    public String readLastSequenceFromIndex() {
        client.admin().indices().prepareRefresh(riverConfig.getRiverIndexName()).execute().actionGet();

        GetResponse lastSeqResponse = client.prepareGet(riverConfig.getRiverIndexName(),
                riverConfig.getRiverName().name(), "_seq").execute().actionGet();

        if (lastSeqResponse.isExists()) {
            return parseLastSeq(lastSeqResponse);
        }
        return null;
    }

    private String parseLastSeq(GetResponse lastSeqResponse) {
        @SuppressWarnings("unchecked")
        Map<String, Object> db = (Map) lastSeqResponse.getSourceAsMap().get(databaseConfig.getDatabase());
        return db == null ? null : (String) db.get(LAST_SEQ);
    }
}
