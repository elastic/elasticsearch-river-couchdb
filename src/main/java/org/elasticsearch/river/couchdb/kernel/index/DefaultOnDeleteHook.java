package org.elasticsearch.river.couchdb.kernel.index;

import org.elasticsearch.action.delete.DeleteRequest;
import java.util.Map;

public class DefaultOnDeleteHook extends OnDeleteHook {

    private final RequestFactory requestFactory;
    private final DocumentHelper helper;

    public DefaultOnDeleteHook(String database, RequestFactory requestFactory, DocumentHelper helper) {
        super(database);
        this.requestFactory = requestFactory;
        this.helper = helper;
    }

    @Override
    public DeleteRequest onDelete(Map<String, Object> document) {
        String id = helper.extractId(document);
        String index = helper.extractIndex(document);
        String type = helper.extractType(document);
        String routing = helper.extractRouting(document);
        String parent = helper.extractParent(document);

        logger.debug("Will delete document with id=[{}].", id);
        return requestFactory.aDeleteRequest(index, type, id, routing, parent);
    }
}
