package org.elasticsearch.river.couchdb.kernel.index;

import org.elasticsearch.action.index.IndexRequest;
import java.util.Map;

public class DefaultOnIndexHook extends OnIndexHook {

    private final RequestFactory requestFactory;
    private final DocumentHelper helper;

    public DefaultOnIndexHook(String database, RequestFactory requestFactory, DocumentHelper helper) {
        super(database);
        this.requestFactory = requestFactory;
        this.helper = helper;
    }

    @Override
    protected boolean shouldBeIndexed(Map<String, Object> document) {
        return true;
    }

    @Override
    protected IndexRequest doIndex(Map<String, Object> document) {
        String id = helper.extractId(document);
        String index = helper.extractIndex(document);
        String type = helper.extractType(document);
        String routing = helper.extractRouting(document);
        String parent = helper.extractParent(document);

        logger.trace("Will index document=[{}].", document);

        return requestFactory.anIndexRequest(index, type, id, document, routing, parent);
    }
}
