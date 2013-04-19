package org.elasticsearch.river.couchdb;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.RiverSettings;
import java.util.Map;

public class IndexConfig {

    public static final String INDEX = "index";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String THROTTLE_SIZE = "throttle_size";
    public static final String BULK_TIMEOUT = "bulk_timeout";
    public static final String BULK_SIZE = "bulk_size";
    public static final String IGNORE_ATTACHMENTS = "ignore_attachments";

    static final TimeValue DEFAULT_BULK_TIMEOUT = timeValueMillis(10);
    static final int DEFAULT_BULK_SIZE = 100;
    static final int DEFAULT_THROTTLE_SIZE = 5 * DEFAULT_BULK_SIZE;
    static final String DEFAULT_INDEX_NAME = "db";

    private String name = DEFAULT_INDEX_NAME;
    private String type = DEFAULT_INDEX_NAME;
    private int bulkSize = DEFAULT_BULK_SIZE;
    private TimeValue bulkTimeout = DEFAULT_BULK_TIMEOUT;
    private int throttleSize = DEFAULT_THROTTLE_SIZE;
    private boolean ignoreAttachments = true;

    public static IndexConfig fromRiverSettings(RiverSettings riverSettings) {
        IndexConfig cfg = new IndexConfig();
        if (riverSettings.settings().containsKey(INDEX)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get(INDEX);
            cfg.name = nodeStringValue(indexSettings.get(NAME), DEFAULT_INDEX_NAME);
            cfg.type = nodeStringValue(indexSettings.get(TYPE), DEFAULT_INDEX_NAME);
            cfg.bulkSize = nodeIntegerValue(indexSettings.get(BULK_SIZE), DEFAULT_BULK_SIZE);
            if (indexSettings.containsKey(BULK_TIMEOUT)) {
                cfg.bulkTimeout = parseTimeValue(nodeStringValue(indexSettings.get(BULK_TIMEOUT), null), DEFAULT_BULK_TIMEOUT);
            }
            cfg.throttleSize = nodeIntegerValue(indexSettings.get(THROTTLE_SIZE), DEFAULT_THROTTLE_SIZE);
            cfg.ignoreAttachments = nodeBooleanValue(indexSettings.get(IGNORE_ATTACHMENTS), true);
        }
        return cfg;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public TimeValue getBulkTimeout() {
        return bulkTimeout;
    }

    public int getThrottleSize() {
        return throttleSize;
    }

    public boolean shouldIgnoreAttachments() {
        return ignoreAttachments;
    }
}
