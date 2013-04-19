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

    private String river = "_river";
    private String type = "couchdb";
    private String db = "db";
    private String index = db;

    private void run() throws Exception {
        node = nodeBuilder().settings(settingsBuilder().put("gateway.type", "local")).node();

        delete(river);
        delete(db);

        try {
            node.client().prepareIndex("_river", "couchdb", "_meta").setSource(meta()).execute().actionGet();

            Thread.sleep(1000000);
        } finally {
            node.stop();
        }
    }

    private XContentBuilder meta() throws IOException {
        return jsonBuilder().startObject()
                    .field("type", type)
                    .startObject("index").field("name", index).field("type", type).field("ignore_attachments", true).endObject()
                    .startObject("couchdb_connection").field("url", "http://localhost:5984").endObject()
                    .startObject("couchdb_database").field("database", db).endObject()
                    .endObject();
    }

    private void delete(String resource) throws InterruptedException {
        try {
            node.client().admin().indices().delete(new DeleteIndexRequest(resource)).actionGet();
        } catch (IndexMissingException itsOk) {}

        Thread.sleep(100);
    }
}
