package org.elasticsearch.river.couchdb;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.river.couchdb.IndexConfig.BULK_SIZE;
import static org.elasticsearch.river.couchdb.IndexConfig.BULK_TIMEOUT;
import static org.elasticsearch.river.couchdb.IndexConfig.DEFAULT_BULK_SIZE;
import static org.elasticsearch.river.couchdb.IndexConfig.DEFAULT_BULK_TIMEOUT;
import static org.elasticsearch.river.couchdb.IndexConfig.DEFAULT_INDEX_NAME;
import static org.elasticsearch.river.couchdb.IndexConfig.DEFAULT_THROTTLE_SIZE;
import static org.elasticsearch.river.couchdb.IndexConfig.INDEX;
import static org.elasticsearch.river.couchdb.IndexConfig.NAME;
import static org.elasticsearch.river.couchdb.IndexConfig.THROTTLE_SIZE;
import static org.elasticsearch.river.couchdb.IndexConfig.TYPE;
import static org.elasticsearch.river.couchdb.IndexConfig.fromRiverSettings;
import static org.fest.assertions.api.Assertions.assertThat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.RiverSettings;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

public class IndexConfigTest {

    private String testName = "testName";
    private String testType = "testType";
    private int testBulkSize = 1337;
    private TimeValue testBulkTimeout = timeValueSeconds(15);
    private int testThrottleSize = 17;

    @Test
    public void shouldParseRiverSettings() {
        // given
        RiverSettings riverSettings = customRiverSettings();

        // when
        IndexConfig cfg = fromRiverSettings(riverSettings);

        // then
        assertThat(cfg.getName()).isEqualTo(testName);
        assertThat(cfg.getType()).isEqualTo(testType);
        assertThat(cfg.getBulkSize()).isEqualTo(testBulkSize);
        assertThat(cfg.getBulkTimeout()).isEqualTo(testBulkTimeout);
        assertThat(cfg.getThrottleSize()).isEqualTo(testThrottleSize);
    }

    @Test
    public void shouldHaveDefaultValues() {
        // given
        RiverSettings riverSettings = new RiverSettings(null, new HashMap<String, Object>());

        IndexConfig cfg = fromRiverSettings(riverSettings);

        // then
        assertThat(cfg.getName()).isEqualTo(DEFAULT_INDEX_NAME);
        assertThat(cfg.getType()).isEqualTo(DEFAULT_INDEX_NAME);
        assertThat(cfg.getBulkSize()).isEqualTo(DEFAULT_BULK_SIZE);
        assertThat(cfg.getBulkTimeout().millis()).isEqualTo(DEFAULT_BULK_TIMEOUT.millis());
        assertThat(cfg.getThrottleSize()).isEqualTo(DEFAULT_THROTTLE_SIZE);
    }

    private RiverSettings customRiverSettings() {
        Map<String, Object> index = newHashMap();
        index.put(NAME, testName);
        index.put(TYPE, testType);
        index.put(BULK_SIZE, testBulkSize);
        index.put(BULK_TIMEOUT, testBulkTimeout);
        index.put(THROTTLE_SIZE, testThrottleSize);
        Map<String, Object> settings = newHashMap();
        settings.put(INDEX, index);
        return new RiverSettings(null, settings);
    }
}
