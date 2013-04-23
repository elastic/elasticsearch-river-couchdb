package org.elasticsearch.river.couchdb;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.river.couchdb.CouchdbDatabaseConfig.COUCHDB_DATABASE;
import static org.elasticsearch.river.couchdb.CouchdbDatabaseConfig.DATABASE;
import static org.elasticsearch.river.couchdb.CouchdbDatabaseConfig.DEFAULT_DATABASE;
import static org.elasticsearch.river.couchdb.CouchdbDatabaseConfig.FILTER;
import static org.elasticsearch.river.couchdb.CouchdbDatabaseConfig.FILTER_PARAMS;
import static org.elasticsearch.river.couchdb.CouchdbDatabaseConfig.SCRIPT;
import static org.elasticsearch.river.couchdb.CouchdbDatabaseConfig.SCRIPT_TYPE;
import static org.elasticsearch.river.couchdb.CouchdbDatabaseConfig.fromRiverSettings;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class CouchdbDatabaseConfigTest {

    @Mock
    private ScriptService scriptService;

    private String testDatabase = "dbname";
    private String testScript = "bar";
    private String testScriptType = "mvel";

    @Test
    public void shouldParseBasicSettings() {
        // given
        RiverSettings riverSettings = customRiverSettings();

        // when
        CouchdbDatabaseConfig cfg = fromRiverSettings(riverSettings);

        // then
        assertThat(cfg.getDatabase()).isEqualTo(testDatabase);
    }

    @Test
    public void shouldParseFiltersSettings() {
        // given
        RiverSettings riverSettings = customRiverSettings();

        // when
        CouchdbDatabaseConfig cfg = fromRiverSettings(riverSettings);

        // then
        assertThat(cfg.shouldUseFilter()).isTrue();
        assertThat(cfg.buildFilterUrlParams()).isEqualTo("&filter=sieve&foo=bar");
    }

    @Test
    public void shouldParseScriptSettings() {
        // given
        RiverSettings riverSettings = customRiverSettings();

        // when
        CouchdbDatabaseConfig cfg = fromRiverSettings(riverSettings);

        // then
        assertThat(cfg.shouldUseScript()).isTrue();

        // and when
        cfg.getScript(scriptService);

        // then
        verify(scriptService).executable(eq(testScriptType), eq(testScript), any(Map.class));
    }

    @Test
    public void shouldHaveDefaultValues() {
        // given
        RiverSettings riverSettings = new RiverSettings(null, new HashMap<String, Object>());

        // when
        CouchdbDatabaseConfig cfg = fromRiverSettings(riverSettings);

        // then
        assertThat(cfg.shouldUseScript()).isFalse();
        assertThat(cfg.shouldUseFilter()).isFalse();
        assertThat(cfg.getDatabase()).isEqualTo(DEFAULT_DATABASE);
    }

    private RiverSettings customRiverSettings() {
        Map<String, Object> couchdbDatabase = newHashMap();
        couchdbDatabase.put(DATABASE, testDatabase);
        couchdbDatabase.put(FILTER, "sieve");
        Map<String, String> filterParams = newHashMap();
        filterParams.put("foo", "bar");
        couchdbDatabase.put(FILTER_PARAMS, filterParams);
        couchdbDatabase.put(SCRIPT, testScript);
        couchdbDatabase.put(SCRIPT_TYPE, testScriptType);
        Map<String, Object> settings = newHashMap();
        settings.put(COUCHDB_DATABASE, couchdbDatabase);
        return new RiverSettings(null, settings);
    }

}
