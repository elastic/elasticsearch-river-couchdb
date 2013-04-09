package org.elasticsearch.river.couchdb;

import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class RiverConfig {
    // TODO figure out what's the difference between all those river names; smells like duplication
    private final RiverName riverName;
    private final RiverSettings riverSettings;
    private final String riverIndexName;

    public RiverConfig(RiverName riverName, RiverSettings riverSettings, String riverIndexName) {
        this.riverName = riverName;
        this.riverSettings = riverSettings;
        this.riverIndexName = riverIndexName;
    }

    public RiverName getRiverName() {
        return riverName;
    }

    public RiverSettings getRiverSettings() {
        return riverSettings;
    }

    public String getRiverIndexName() {
        return riverIndexName;
    }
}
