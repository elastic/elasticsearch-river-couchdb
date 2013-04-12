package org.elasticsearch.river.couchdb;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.river.couchdb.CouchdbConnectionConfig.*;
import static org.elasticsearch.river.couchdb.util.Helpers.asUrl;
import static org.fest.assertions.api.Assertions.assertThat;
import org.elasticsearch.river.RiverSettings;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

public class CouchdbConnectionConfigTest {

    private String testUrl = "https://127.0.0.1:1337";

    @Test
    public void shouldParseRiverSettings() {
        // given
        RiverSettings riverSettings = customRiverSettings();

        // when
        CouchdbConnectionConfig cfg = fromRiverSettings(riverSettings);

        // then
        assertThat(cfg.getUrl()).isEqualTo(asUrl(testUrl));

        assertThat(cfg.requiresAuthentication()).isTrue();
        assertThat(cfg.getBasicAuthHeader()).isEqualTo("Basic d2F0bWFuOndhdHdhdHdhdCE=");

        assertThat(cfg.shouldVerifyHostname()).isFalse();
    }

    @Test
    public void shouldHaveDefaultValues() {
        // given
        RiverSettings riverSettings = new RiverSettings(null, new HashMap<String, Object>());

        // when
        CouchdbConnectionConfig cfg = fromRiverSettings(riverSettings);

        // then
        assertThat(cfg.getUrl()).isEqualTo(asUrl(DEFAULT_URL));
        assertThat(cfg.requiresAuthentication()).isFalse();
        assertThat(cfg.shouldVerifyHostname()).isTrue();
    }

    private RiverSettings customRiverSettings() {
        Map<String, Object> couchdbConnection = newHashMap();
        couchdbConnection.put(URL, testUrl);
        couchdbConnection.put(NO_VERIFY, true);
        couchdbConnection.put(USERNAME, "watman");
        couchdbConnection.put(PASSWORD, "watwatwat!");
        Map<String, Object> settings = newHashMap();
        settings.put(COUCHDB_CONNECTION, couchdbConnection);
        return new RiverSettings(null, settings);
    }

}
