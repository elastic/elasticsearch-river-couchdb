package org.elasticsearch.river.couchdb;

import static org.elasticsearch.common.base.Optional.absent;
import static org.elasticsearch.common.base.Optional.fromNullable;
import static org.elasticsearch.river.couchdb.util.LoggerHelper.slurperLogger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.annotations.VisibleForTesting;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.logging.ESLogger;
import java.util.Map;

public class LastSeqReader {

    public static final String LAST_SEQ = "last_seq";

    private final ESLogger logger;

    private final CouchdbDatabaseConfig databaseConfig;
    private final RiverConfig riverConfig;

    private final Client client;

    public LastSeqReader(CouchdbDatabaseConfig databaseConfig, RiverConfig riverConfig, Client client) {
        this.databaseConfig = databaseConfig;
        this.riverConfig = riverConfig;
        this.client = client;

        logger = slurperLogger(LastSeqReader.class, databaseConfig.getDatabase());
    }

    public Optional<String> readLastSequenceFromIndex() {
        refreshIndex();

        GetResponse lastSeqResponse = doReadLastSeq();

        if (lastSeqResponse.isExists()) {
            String lastSeq = parseLastSeq(lastSeqResponse);
            logger.info("Read {}=[{}] in index.", LAST_SEQ, lastSeq);
            return fromNullable(lastSeq);
        }
        logger.info("No {} value found in index.", LAST_SEQ);
        return absent();
    }

    @VisibleForTesting
    void refreshIndex() {
        client.admin().indices().prepareRefresh(riverConfig.getRiverIndexName()).execute().actionGet();
    }

    @VisibleForTesting
    GetResponse doReadLastSeq() {
        return client.prepareGet(riverConfig.getRiverIndexName(),
                riverConfig.getRiverName().name(), "_seq").execute().actionGet();
    }

    private String parseLastSeq(GetResponse lastSeqResponse) {
        @SuppressWarnings("unchecked")
        Map<String, Object> db = (Map) lastSeqResponse.getSourceAsMap().get(databaseConfig.getDatabase());
        return db == null ? null : (String) db.get(LAST_SEQ);
    }
}
