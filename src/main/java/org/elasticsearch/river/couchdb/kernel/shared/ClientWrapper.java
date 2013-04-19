package org.elasticsearch.river.couchdb.kernel.shared;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;

public class ClientWrapper {

    private final Client client;

    public ClientWrapper(Client client) {
        this.client = client;
    }

    public GetResponse read(String type, String name, String id) {
        return client.prepareGet(type, name, id).execute().actionGet();
    }

    public RefreshResponse refreshIndex(String index) {
        return client.admin().indices().prepareRefresh(index).execute().actionGet();
    }

    public CreateIndexResponse createIndex(String name) {
        return client.admin().indices().prepareCreate(name).execute().actionGet();
    }

    public BulkRequestBuilder prepareBulkRequest() {
        return client.prepareBulk();
    }

    public BulkResponse executeBulkRequest(BulkRequestBuilder bulk) {
        return bulk.execute().actionGet();
    }
}
