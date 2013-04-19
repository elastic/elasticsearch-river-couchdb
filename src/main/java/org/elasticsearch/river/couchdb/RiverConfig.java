package org.elasticsearch.river.couchdb;

import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class RiverConfig {

    private final RiverName riverName;
    private final String riverIndexName;

    public RiverConfig(RiverName riverName, String riverIndexName) {
        this.riverName = riverName;
        this.riverIndexName = riverIndexName;
    }

    public RiverName getRiverName() {
        return riverName;
    }

    public String getRiverIndexName() {
        return riverIndexName;
    }
}
