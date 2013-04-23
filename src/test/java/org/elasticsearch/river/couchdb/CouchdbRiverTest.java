package org.elasticsearch.river.couchdb;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import java.io.IOException;

public class CouchdbRiverTest {

    public static void main(String[] args) throws Exception {
        new CouchdbRiverTest().run();
    }

    private Node node;

    private String dbName = "db";
    private String indexName = dbName;

    private void run() throws Exception {
        node = nodeBuilder().settings(settingsBuilder().put("gateway.type", "local")).node();

        delete("_river");
        delete(indexName);

        try {
            node.client().prepareIndex("_river", "couchdb", "_meta").setSource(meta()).execute().actionGet();

            Thread.sleep(1000000);
        } finally {
            node.stop();
        }
    }

    private XContentBuilder meta() throws IOException {
        return jsonBuilder().startObject()
                    .field("type", "couchdb")
                    .startObject("index").field("name", indexName).field("type", indexName).field("ignore_attachments", true).endObject()
                    .startObject("couchdb_connection").field("url", "http://localhost:5984").endObject()
                    .startObject("couchdb_database").field("database", dbName).endObject()
                    .endObject();
    }

    private void delete(String resource) throws InterruptedException {
        try {
            node.client().admin().indices().delete(new DeleteIndexRequest(resource)).actionGet();
        } catch (IndexMissingException itsOk) {}

        Thread.sleep(100);
    }
}
