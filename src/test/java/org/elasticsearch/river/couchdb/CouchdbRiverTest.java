package org.elasticsearch.river.couchdb;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class CouchdbRiverTest {

    public static void main(String[] args) throws Exception {
        Node node = nodeBuilder().settings(settingsBuilder().put("gateway.type", "local")).node();
        Thread.sleep(1000);

        XContentBuilder src = jsonBuilder().startObject()
                    .field("type", "couchdb")
                    .startObject("index")
                        .field("name", "db")
                        .field("type", "couchdb")
                        .field("ignore_attachments", true)
                    .endObject()
                    .startObject("couchdb_connection")
                        .field("url", "http://localhost:5984")
                    .endObject()
                    .startObject("couchdb_database")
                        .field("database", "db")
                    .endObject()
                .endObject();

        try {
            node.client().prepareIndex("_river", "couchdb", "_meta").setSource(src).execute().actionGet();

            Thread.sleep(1000000);
        } finally {
            node.stop();
        }
    }
}
