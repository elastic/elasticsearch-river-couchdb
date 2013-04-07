package org.elasticsearch.river.couchdb;

import static org.elasticsearch.common.base.Preconditions.checkState;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;
import static org.elasticsearch.river.couchdb.Helpers.asUrl;
import static org.elasticsearch.river.couchdb.Helpers.nullToEmpty;
import org.elasticsearch.common.Base64;
import org.elasticsearch.river.RiverSettings;
import java.net.URL;
import java.util.Map;

public class CouchdbConnectionConfig {

    public static final String COUCHDB_CONNECTION = "couchdb_connection";
    public static final String URL = "url";
    public static final String NO_VERIFY = "no_verify";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    private static final String DEFAULT_URL = "http://localhost:5984";

    private URL url = asUrl(DEFAULT_URL);
    private String username;
    private String password;
    private boolean noVerify;

    public static CouchdbConnectionConfig fromRiverSettings(RiverSettings riverSettings) {
        CouchdbConnectionConfig cfg = new CouchdbConnectionConfig();

        if (riverSettings.settings().containsKey(COUCHDB_CONNECTION)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> couchdbConnection = (Map<String, Object>) riverSettings.settings().get(COUCHDB_CONNECTION);

            cfg.url = asUrl(nodeStringValue(couchdbConnection.get(URL), DEFAULT_URL));
            cfg.noVerify = nodeBooleanValue(couchdbConnection.get(NO_VERIFY));

            if (couchdbConnection.containsKey(USERNAME)) {
                cfg.username = nodeStringValue(couchdbConnection.get(USERNAME), null);
                cfg.password = nodeStringValue(couchdbConnection.get(PASSWORD), null);
            }
        }
        return cfg;
    }

    public URL getUrl() {
        return url;
    }

    public boolean requiresAuthentication() {
        return username != null;
    }

    public String getBasicAuthHeader() {
        checkState(requiresAuthentication());
        return "Basic " + Base64.encodeBytes((username + ":" + nullToEmpty(password)).getBytes());
    }

    public boolean shouldVerifyHostname() {
        return !noVerify;
    }
}
