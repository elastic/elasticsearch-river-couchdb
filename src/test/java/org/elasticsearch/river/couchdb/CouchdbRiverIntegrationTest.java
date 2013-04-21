package org.elasticsearch.river.couchdb;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.FIVE_HUNDRED_MILLISECONDS;
import static com.jayway.awaitility.Duration.TEN_SECONDS;
import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.api.Assertions.entry;
import static org.junit.Assume.assumeTrue;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.DbAccessException;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.concurrent.Callable;

public class CouchdbRiverIntegrationTest {

    private static final String DOCUMENT_ID = "id";
    private static final String CONTENTS_KEY = "contents";
    private static final String CONTENTS_VALUE = "abc";

    private HttpClient httpClient;
    private CouchDbInstance instance;

    private Node node;

    private String couchdbUrl = "http://localhost:5984";
    private String dbName = "couchdb_river_test_db";
    private String indexName = dbName;

    @Before
    public void setUp() throws MalformedURLException {
        httpClient = new StdHttpClient.Builder().url(couchdbUrl).build();
        instance = new StdCouchDbInstance(httpClient);

        assumeTrue(canConnectToCouchdb());

        tryDeleteDatabase();

        node = nodeBuilder().settings(settingsBuilder()
                .put("node.http.enabled", false)
                .put("index.gateway.type", "none")
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)).node();
    }

    private boolean canConnectToCouchdb() {
        try {
            instance.getAllDatabases();
            return true;
        } catch (DbAccessException dbae) {
            return false;
        }
    }

    @After
    public void tearDown() {
        tryDeleteDatabase();

        httpClient.shutdown();

        if (node != null) {
            node.stop();
        }
    }

    private void tryDeleteDatabase() {
        try {
            instance.deleteDatabase(dbName);
        } catch (Exception itsOk) {}
    }

    @Test
    public void shouldFindADocumentInIndexAfterItWasCreatedInCouchdbDatabase() throws Exception {
        // given
        CouchDbConnector db = givenAnEmptyCouchdbDatabase();

        givenInitializedIndex();

        // when Couchdb document created
        Map<String, Object> doc = newHashMap();
        doc.put(CONTENTS_KEY, CONTENTS_VALUE);
        db.create(DOCUMENT_ID, doc);

        // and when it's indexed in elasticsearch
        final SearchRequestBuilder searchRequestBuilder = searchRequestBuilder();
        waitUntilDocumentIsIndexed(searchRequestBuilder);

        // then it contains all properties
        SearchResponse result = searchRequestBuilder.execute().actionGet();
        assertThat(result.getHits().getAt(0).getSource()).contains(entry("_id", DOCUMENT_ID), entry("contents", "abc"));
    }

    private CouchDbConnector givenAnEmptyCouchdbDatabase() {
        CouchDbConnector dbConnector = new StdCouchDbConnector(dbName, instance);
        dbConnector.createDatabaseIfNotExists();
        return dbConnector;
    }

    private void givenInitializedIndex() throws InterruptedException, IOException {
        deleteIndex("_river");
        deleteIndex(indexName);

        node.client().prepareIndex("_river", "couchdb", "_meta").setSource(meta()).execute().actionGet();

        waitUntilIndexIsCreated();
    }

    private void deleteIndex(String resource) throws InterruptedException {
        try {
            node.client().admin().indices().delete(new DeleteIndexRequest(resource)).actionGet();
        } catch (IndexMissingException itsOk) {}
    }

    private XContentBuilder meta() throws IOException {
        return jsonBuilder().startObject()
                .field("type", "couchdb")
                .startObject("index").field("name", indexName).field("type", indexName).field("ignore_attachments", true).endObject()
                .startObject("couchdb_connection").field("url", couchdbUrl).endObject()
                .startObject("couchdb_database").field("database", dbName).endObject()
                .endObject();
    }

    private void waitUntilIndexIsCreated() {
        await().atMost(TEN_SECONDS).pollDelay(FIVE_HUNDRED_MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    node.client().admin().indices().prepareRefresh(indexName).execute().actionGet();
                    return true;
                } catch (IndexMissingException ime) {
                    return false;
                }
            }
        });
    }

    private SearchRequestBuilder searchRequestBuilder() {
        return node.client().prepareSearch(indexName).setTypes(indexName)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(termQuery(CONTENTS_KEY, CONTENTS_VALUE))
                .setExplain(true);
    }

    private void waitUntilDocumentIsIndexed(final SearchRequestBuilder searchRequestBuilder) {
        await().atMost(TEN_SECONDS).pollDelay(FIVE_HUNDRED_MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                SearchResponse response = searchRequestBuilder.execute().actionGet();
                return response.getHits().getHits() != null && response.getHits().getHits().length > 0;
            }
        });
    }
}
