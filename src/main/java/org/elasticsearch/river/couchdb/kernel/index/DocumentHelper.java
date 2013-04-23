package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.common.base.Objects.firstNonNull;
import org.elasticsearch.river.couchdb.IndexConfig;
import java.util.Map;

public class DocumentHelper {

    private final IndexConfig indexConfig;

    public DocumentHelper(IndexConfig indexConfig) {
        this.indexConfig = indexConfig;
    }

    public String extractId(Map<String, Object> doc) {
        return (String) doc.get("_id");
    }

    public String extractIndex(Map<String, Object> doc) {
        return firstNonNull((String) doc.get("_index"), indexConfig.getName());
    }

    public String extractType(Map<String, Object> doc) {
        return firstNonNull((String) doc.get("_type"), indexConfig.getType());
    }

    public String extractRouting(Map<String, Object> doc) {
        return (String) doc.get("_routing");
    }

    public String extractParent(Map<String, Object> doc) {
        return (String) doc.get("_parent");
    }
}
