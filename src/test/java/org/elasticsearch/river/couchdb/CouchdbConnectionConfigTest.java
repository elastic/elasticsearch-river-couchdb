package org.elasticsearch.river.couchdb;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.river.couchdb.CouchdbConnectionConfig.*;
import static org.elasticsearch.river.couchdb.Helpers.asUrl;
import static org.fest.assertions.api.Assertions.assertThat;
import org.elasticsearch.river.RiverSettings;
import org.junit.Test;
import java.util.Map;

public class CouchdbConnectionConfigTest {

    private String testUrl = "https://127.0.0.1:1337";
    private Boolean testNoVerify = true;
    private String testUsername = "watman";
    private String testPassword = "watwatwat!";

    @Test
    public void shouldParseRiverSettings() {
        // given
        RiverSettings riverSettings = customRiverSettings();

        // when
        CouchdbConnectionConfig cfg = fromRiverSettings(riverSettings);

        // then
        assertThat(cfg.requiresAuthentication()).isTrue();
        assertThat(cfg.getUrl()).isEqualTo(asUrl(testUrl));
        assertThat(cfg.shouldVerifyHostname()).isFalse();
        assertThat(cfg.getBasicAuthHeader()).isEqualTo("Basic d2F0bWFuOndhdHdhdHdhdCE=");
    }

    private RiverSettings customRiverSettings() {
        Map<String, Object> couchdbConnection = newHashMap();
        couchdbConnection.put(URL, testUrl);
        couchdbConnection.put(NO_VERIFY, testNoVerify);
        couchdbConnection.put(USERNAME, testUsername);
        couchdbConnection.put(PASSWORD, testPassword);
        Map<String, Object> settings = newHashMap();
        settings.put(COUCHDB_CONNECTION, couchdbConnection);
        return new RiverSettings(null, settings);
    }

}
