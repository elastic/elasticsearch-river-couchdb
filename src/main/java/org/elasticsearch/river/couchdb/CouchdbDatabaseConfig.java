package org.elasticsearch.river.couchdb;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;
import static org.elasticsearch.river.couchdb.Helpers.asUrlParam;
import java.util.Map;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

public class CouchdbDatabaseConfig {

    public static final String COUCHDB_DATABASE = "couchdb_database";

    public static final String DATABASE = "database";
    public static final String FILTER = "filter";
    public static final String FILTER_PARAMS = "filter_params";
    public static final String SCRIPT = "script";
    public static final String SCRIPT_TYPE = "scriptType";
    public static final String IGNORE_ATTACHMENTS = "ignore_attachments";

    private static final String DEFAULT_DATABASE = "db";

    private String database = DEFAULT_DATABASE;
    private boolean ignoreAttachments = true;

    private String filter;
    private Map<String, String> filterParams;

    private String script;
    private String scriptType;

    @SuppressWarnings("unchecked")
    public static CouchdbDatabaseConfig fromRiverSettings(RiverSettings riverSettings) {
        CouchdbDatabaseConfig cfg = new CouchdbDatabaseConfig();

        if (riverSettings.settings().containsKey(COUCHDB_DATABASE)) {
            Map<String, Object> couchSettings = (Map<String, Object>) riverSettings.settings().get(COUCHDB_DATABASE);

            cfg.database = nodeStringValue(couchSettings.get(DATABASE), DEFAULT_DATABASE);

            if (couchSettings.containsKey(FILTER)) {
                cfg.filter = nodeStringValue(couchSettings.get(FILTER), null);
                cfg.filterParams = (Map<String, String>) couchSettings.get(FILTER_PARAMS);
            }

            cfg.ignoreAttachments = nodeBooleanValue(couchSettings.get(IGNORE_ATTACHMENTS));

            if (couchSettings.containsKey(SCRIPT)) {
                cfg.script = nodeStringValue(couchSettings.get(SCRIPT), null);
                cfg.scriptType = nodeStringValue(couchSettings.get(SCRIPT_TYPE), "js");
            }
        }
        return cfg;
    }

    public String getDatabase() {
        return database;
    }

    public boolean shouldIgnoreAttachments() {
        return ignoreAttachments;
    }
    
    public boolean shouldUseFilter() {
        return filter != null;
    }
    
    public String buildFilterUrlParams() {
        if (!shouldUseFilter()) {
            return "";
        }
        StringBuilder sb = new StringBuilder("&").append(asUrlParam("filter", filter));
        for (Map.Entry<String, String> entry : filterParams.entrySet()) {
            sb.append("&").append(asUrlParam(entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }
    
    public boolean shouldUseScript() {
        return script != null;
    }
    
    public ExecutableScript getScript(ScriptService scriptService) {
        return scriptService.executable(scriptType, script, newHashMap());
    }
}
