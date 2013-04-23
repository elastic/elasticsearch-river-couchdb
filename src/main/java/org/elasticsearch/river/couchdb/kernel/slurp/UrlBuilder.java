package org.elasticsearch.river.couchdb.kernel.slurp;

import static org.elasticsearch.common.base.Optional.absent;
import static org.elasticsearch.river.couchdb.util.Helpers.asUrl;
import static org.elasticsearch.river.couchdb.util.Helpers.toUtf8;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.river.couchdb.CouchdbConnectionConfig;
import org.elasticsearch.river.couchdb.CouchdbDatabaseConfig;
import java.net.URL;

public class UrlBuilder {

    private final CouchdbConnectionConfig connectionConfig;
    private final CouchdbDatabaseConfig databaseConfig;

    private Optional<String> lastSeq = absent();

    public UrlBuilder(CouchdbConnectionConfig connectionConfig, CouchdbDatabaseConfig databaseConfig) {
        this.connectionConfig = connectionConfig;
        this.databaseConfig = databaseConfig;
    }

    public URL build() {
        StringBuilder sb = new StringBuilder(commonUrlBase());

        if (lastSeq.isPresent()) {
            sb.append("&since=").append(toUtf8(lastSeq.get()));
        }

        return asUrl(sb.toString());
    }

    private String commonUrlBase() {
        StringBuilder sb = new StringBuilder(connectionConfig.getUrl().toString()).append("/")
                .append(databaseConfig.getDatabase())
                .append("/_changes?feed=continuous&include_docs=true&heartbeat=30000");

        if (databaseConfig.shouldUseFilter()) {
            sb.append(databaseConfig.buildFilterUrlParams());
        }
        return sb.toString();
    }

    public UrlBuilder withLastSeq(Optional<String> lastSeq) {
        this.lastSeq = lastSeq;
        return this;
    }
}
