/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.couchdb;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.river.couchdb.helper.CouchDBClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.river.couchdb.helper.CouchDBClient.putDocument;
import static org.elasticsearch.river.couchdb.helper.CouchDBClient.putDocumentWithAttachments;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for CouchDb river<br>
 * You may have a couchdb instance running on localhost:5984 with a mytest database.
 */
@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.SUITE,
        numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class CouchdbRiverIntegrationTest extends AbstractCouchdbTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
            .build();
    }

    private interface InjectorHook {
        public void inject();
    }

    private static final String testDbPrefix = "elasticsearch_couch_test_";

    private String suffix;

    @Before
    public final void dbSuffixName() {
        suffix = String.valueOf(System.nanoTime()) + "_" + randomInt();
    }

    @After
    public final void wipeDbAfterTest() {
        logger.info("  -> Removing test database [{}]", getDbName());
        try {
            CouchDBClient.dropTestDatabase(getDbName());
        } catch (Exception e) {
            // Let's ignore it
        }
    }

    private String getDbName() {
        return testDbPrefix.concat(Strings.toUnderscoreCase(getTestName())).concat(suffix);
    }

    private void launchTest(XContentBuilder river, final Integer numDocs, InjectorHook injectorHook)
            throws IOException, InterruptedException {
        logger.info("  -> Checking couchdb running");
        CouchDBClient.checkCouchDbRunning();
        logger.info("  -> Creating test database [{}]", getDbName());
        CouchDBClient.dropAndCreateTestDatabase(getDbName());
        logger.info("  -> Put [{}] documents", numDocs);
        for (int i = 0; i < numDocs; i++) {
            CouchDBClient.putDocument(getDbName(), "" + i, "foo", "bar", "content", "" + i);
        }
        logger.info("  -> Put [{}] documents done", numDocs);

        if (injectorHook != null) {
            logger.info("  -> Injecting extra data");
            injectorHook.inject();
        }

        logger.info("  -> Create river");
        try {
            createIndex(getDbName());
        } catch (IndexAlreadyExistsException e) {
            // No worries. We already created the index before
        }
        index("_river", getDbName(), "_meta", river);

        logger.info("  -> Wait for some docs");
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    refresh();
                    SearchResponse response = client().prepareSearch(getDbName()).get();
                    logger.info("  -> got {} docs in {} index", response.getHits().totalHits(), getDbName());
                    return response.getHits().totalHits() == numDocs;
                } catch (IndexMissingException e) {
                    return false;
                }
            }
        }, 1, TimeUnit.MINUTES), equalTo(true));
    }

    /**
     * This is a simple test case for testing attachments removing.
     */
    @Test
    public void testAttachmentEnabled() throws IOException, InterruptedException {
        runAttachmentTest(false);
    }

    /**
     * This is a simple test case for testing attachments removing.
     */
    @Test
    public void testAttachmentDisabled() throws IOException, InterruptedException {
        runAttachmentTest(true);
    }

    private void runAttachmentTest(boolean disabled) throws IOException, InterruptedException {
        // Create the river
        launchTest(jsonBuilder()
            .startObject()
                .field("type", "couchdb")
                .startObject("couchdb")
                    .field("host", CouchDBClient.host)
                    .field("port", CouchDBClient.port)
                    .field("db", getDbName())
                    .field("ignore_attachments", disabled)
                .endObject()
            .endObject(), 0, new InjectorHook() {
            @Override
            public void inject() {
                try {
                    putDocumentWithAttachments(getDbName(), "1",
                            new ImmutableList.Builder<String>().add("foo", "bar").build(),
                            "text-in-english.txt", "God save the queen!",
                            "text-in-french.txt", "Allons enfants !");
                } catch (IOException e) {
                    logger.error("Error while injecting attachments");
                }

            }
        });

        // Check that docs are indexed by the river
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    refresh();
                    SearchResponse response = client().prepareSearch(getDbName()).get();
                    logger.info("  -> got {} docs in {} index", response.getHits().totalHits(), getDbName());
                    return response.getHits().totalHits() == 1;
                } catch (IndexMissingException e) {
                    return false;
                }
            }
        }, 1, TimeUnit.MINUTES), equalTo(true));

        SearchResponse response = client().prepareSearch(getDbName())
                .addField("_attachments.text-in-english.txt.content_type")
                .addField("_attachments.text-in-french.txt.content_type")
                .get();

        assertThat(response.getHits().getAt(0).field("_attachments.text-in-english.txt.content_type"), disabled ? nullValue() : notNullValue());
        assertThat(response.getHits().getAt(0).field("_attachments.text-in-french.txt.content_type"), disabled ? nullValue() : notNullValue());
        if (!disabled) {
            assertThat(response.getHits().getAt(0).field("_attachments.text-in-english.txt.content_type").getValue().toString(), is("text/plain"));
            assertThat(response.getHits().getAt(0).field("_attachments.text-in-french.txt.content_type").getValue().toString(), is("text/plain"));
        }
    }

    @Test
    public void testParameters() throws IOException, InterruptedException {
        launchTest(jsonBuilder()
                        .startObject()
                            .field("type", "couchdb")
                            .startObject("couchdb")
                                .field("heartbeat", "5s")
                                .field("read_timeout", "15s")
                            .endObject()
                        .endObject(), randomIntBetween(5, 1000), null);
    }

    @Test
    public void testSimple() throws IOException, InterruptedException {
        launchTest(jsonBuilder()
                .startObject()
                .field("type", "couchdb")
                .endObject(), randomIntBetween(5, 1000), null);
    }

    @Test
    public void testScriptingDefaultEngine() throws IOException, InterruptedException {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "couchdb")
                    .startObject("couchdb")
                        .field("script", "ctx.doc.newfield = ctx.doc.foo")
                    .endObject()
                .endObject(), randomIntBetween(5, 1000), null);

        SearchResponse response = client().prepareSearch(getDbName())
                .addField("newfield")
                .get();

        assertThat(response.getHits().getAt(0).field("newfield"), notNullValue());
        assertThat(response.getHits().getAt(0).field("newfield").getValue().toString(), is("bar"));
    }

    /**
     * Test case for #44: https://github.com/elasticsearch/elasticsearch-river-couchdb/issues/44
     */
    @Test
    public void testScriptingQuote_44() throws IOException, InterruptedException {
        launchTest(jsonBuilder()
                .startObject()
                .field("type", "couchdb")
                .startObject("couchdb")
                .field("script", "ctx.doc.newfield = 'value1'")
                .endObject()
                .endObject(), randomIntBetween(5, 1000), null);

        SearchResponse response = client().prepareSearch(getDbName())
                .addField("newfield")
                .get();

        assertThat(response.getHits().getAt(0).field("newfield"), notNullValue());
        assertThat(response.getHits().getAt(0).field("newfield").getValue().toString(), is("value1"));
    }

    /**
     * Test case for #51: https://github.com/elasticsearch/elasticsearch-river-couchdb/issues/51
     */
    @Test
    public void testScriptingParentChild_51() throws IOException, InterruptedException, ExecutionException {
        prepareCreate(getDbName())
                .addMapping("region", jsonBuilder()
                        .startObject()
                            .startObject("region")
                            .endObject()
                        .endObject().string())
                .addMapping("campus", jsonBuilder()
                        .startObject()
                            .startObject("campus")
                                .startObject("_parent")
                                    .field("type", "region")
                                .endObject()
                            .endObject()
                        .endObject().string())
                .get();

        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "couchdb")
                    .startObject("couchdb")
                        .field("script", "ctx._type = ctx.doc.type; if (ctx._type == 'campus') { ctx._parent = ctx.doc.parent_id; }")
                    .endObject()
                .endObject(), 0, new InjectorHook() {
            @Override
            public void inject() {
                try {
                    putDocument(getDbName(), "1",
                            "type", "region",
                            "name", "bretagne");
                    putDocument(getDbName(), "2",
                            "type", "campus",
                            "name", "enib",
                            "parent_id", "1");
                } catch (IOException e) {
                    logger.error("Error while injecting documents");
                }
            }
        });

        // Check that docs are indexed by the river
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    refresh();
                    SearchResponse response = client().prepareSearch(getDbName()).get();
                    logger.info("  -> got {} docs in {} index", response.getHits().totalHits(), getDbName());
                    return response.getHits().totalHits() == 2;
                } catch (IndexMissingException e) {
                    return false;
                }
            }
        }, 1, TimeUnit.MINUTES), equalTo(true));

        SearchResponse response = client().prepareSearch(getDbName())
                .setQuery(
                        QueryBuilders.hasChildQuery("campus",
                                QueryBuilders.matchQuery("name", "enib"))
                )
                .get();

        assertThat(response.getHits().getTotalHits(), is(1L));
        assertThat(response.getHits().getAt(0).getType(), is("region"));
        assertThat(response.getHits().getAt(0).getId(), is("1"));
    }

    /**
     * Test case for #45: https://github.com/elasticsearch/elasticsearch-river-couchdb/issues/45
     */
    @Test
    public void testScriptingTypeOf_45() throws IOException, InterruptedException {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "couchdb")
                    .startObject("couchdb")
                        .field("script_type", "groovy")
                        // This groovy script removes all "_id" fields and 50% of "content" fields
                        .field("script", " def docId = Integer.parseInt(ctx.doc[\"_id\"]);\n" +
                                " def removals = [\"content\", \"_id\"]; \n" +
                                " for(i in removals) { \n" +
                                "\tif (ctx.doc.containsKey(i)) { \n" +
                                "\t\tif (\"content\".equals(i)) {\n" +
                                "\t\t\tif ((docId % 2) == 0) {\n" +
                                "\t\t\t\tctx.doc.remove(i)\n" +
                                "\t\t\t} \n" +
                                "\t\t} else {\n" +
                                "\t\t\tctx.doc.remove(i)\n" +
                                "\t\t}\n" +
                                "\t} \n" +
                                "}")
                    .endObject()
                .endObject(), randomIntBetween(5, 1000), null);

        int nbOfResultsToCheck = 100;

        SearchResponse response = client().prepareSearch(getDbName())
                .addField("foo")
                .addField("content")
                .addField("_id")
                .setSize(nbOfResultsToCheck)
                .get();

        for (int i=0; i < Math.min(response.getHits().getTotalHits(), nbOfResultsToCheck); i++) {
            SearchHit hit = response.getHits().getAt(i);
            int docId = Integer.parseInt(hit.getId());

            assertThat(hit.field("foo"), notNullValue());
            assertThat(hit.field("foo").getValue().toString(), is("bar"));
            assertThat(hit.field("_id"), nullValue());
            if ((docId % 2) == 0) {
                assertThat(hit.field("content"), nullValue());
            } else {
                assertThat(hit.field("content").getValue().toString(), is(hit.getId()));
            }
        }
    }

    /**
     * Test case for #66: https://github.com/elasticsearch/elasticsearch-river-couchdb/issues/66
     */
    @Test
    public void testClosingWhileIndexing_66() throws IOException, InterruptedException {
        final int nbDocs = 10;
        logger.info("  -> Checking couchdb running");
        CouchDBClient.checkCouchDbRunning();
        logger.info("  -> Creating test database [{}]", getDbName());
        CouchDBClient.dropAndCreateTestDatabase(getDbName());

        logger.info("  -> Inserting [{}] docs in couchdb", nbDocs);
        for (int i = 0; i < nbDocs; i++) {
            CouchDBClient.putDocument(getDbName(), "" + i, "foo", "bar", "content", "" + i);
        }

        logger.info("  -> Create index");
        try {
            createIndex(getDbName());
        } catch (IndexAlreadyExistsException e) {
            // No worries. We already created the index before
        }

        logger.info("  -> Create river");
        index("_river", getDbName(), "_meta", jsonBuilder()
                .startObject()
                    .field("type", "couchdb")
                    .startObject("couchdb")
                // We use here a script to have a chance to slow down the process and close the river while processing it
                        .field("script", "for (int x = 0; x < 10000000; x++) { x*x*x } ;")
                    .endObject()
                    .startObject("index")
                        .field("flush_interval", "100ms")
                    .endObject()
                .endObject());

        // Check that docs are indexed by the river
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    refresh();
                    SearchResponse response = client().prepareSearch(getDbName()).get();
                    logger.info("  -> got {} docs in {} index", response.getHits().totalHits(), getDbName());
                    return response.getHits().totalHits() == nbDocs;
                } catch (IndexMissingException e) {
                    return false;
                }
            }
        }, 1, TimeUnit.MINUTES), equalTo(true));


        logger.info("  -> Inserting [{}] docs in couchdb", nbDocs);
        for (int i = nbDocs; i < 2*nbDocs; i++) {
            CouchDBClient.putDocument(getDbName(), "" + i, "foo", "bar", "content", "" + i);
        }

        // Check that docs are still processed by the river
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    refresh();
                    SearchResponse response = client().prepareSearch(getDbName()).get();
                    logger.info("  -> got {} docs in {} index", response.getHits().totalHits(), getDbName());
                    return response.getHits().totalHits() > nbDocs;
                } catch (IndexMissingException e) {
                    return false;
                }
            }
        }, 10, TimeUnit.SECONDS), equalTo(true));

        logger.info("  -> Remove river while injecting");
        client().prepareDelete("_river", getDbName(), "_meta").get();

        logger.info("  -> Inserting [{}] docs in couchdb", nbDocs);
        for (int i = 2*nbDocs; i < 3*nbDocs; i++) {
            CouchDBClient.putDocument(getDbName(), "" + i, "foo", "bar", "content", "" + i);
        }

        // Check that docs are indexed by the river
        boolean foundAllDocs = awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    refresh();
                    SearchResponse response = client().prepareSearch(getDbName()).get();
                    logger.info("  -> got {} docs in {} index", response.getHits().totalHits(), getDbName());
                    return response.getHits().totalHits() == 3 * nbDocs;
                } catch (IndexMissingException e) {
                    return false;
                }
            }
        }, 10, TimeUnit.SECONDS);

        // We should not have 30 documents at the end as we removed the river immediately after having
        // injecting 10 more docs in couchdb
        assertThat("We should not have 30 documents as the river is supposed to have been stopped!", foundAllDocs, is(false));

        // We expect seeing a line in logs like:
        // [WARN ][org.elasticsearch.river.couchdb] [node_0] [couchdb][elasticsearch_couch_test_test_closing_while_indexing_66] river was closing while trying to index document [elasticsearch_couch_test_test_closing_while_indexing_66/elasticsearch_couch_test_test_closing_while_indexing_66/11]. Operation skipped.
    }

    /**
     * Test case for #17: https://github.com/elasticsearch/elasticsearch-river-couchdb/issues/17
     */
    @Test
    public void testCreateCouchdbDatabaseWhileRunning_17() throws IOException, InterruptedException {
        final int nbDocs = between(50, 300);
        logger.info("  -> Checking couchdb running");
        CouchDBClient.checkCouchDbRunning();

        logger.info("  -> Create index");
        try {
            createIndex(getDbName());
        } catch (IndexAlreadyExistsException e) {
            // No worries. We already created the index before
        }

        logger.info("  -> Create river");
        index("_river", getDbName(), "_meta", jsonBuilder()
                .startObject()
                    .field("type", "couchdb")
                .endObject());

        // Check that the river is started
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    refresh();
                    GetResponse response = get("_river", getDbName(), "_status");
                    return response.isExists();
                } catch (IndexMissingException e) {
                    return false;
                }
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));

        logger.info("  -> Creating test database [{}]", getDbName());
        CouchDBClient.dropAndCreateTestDatabase(getDbName());

        logger.info("  -> Inserting [{}] docs in couchdb", nbDocs);
        for (int i = 0; i < nbDocs; i++) {
            CouchDBClient.putDocument(getDbName(), "" + i, "foo", "bar", "content", "" + i);
        }

        // Check that docs are still processed by the river
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    refresh();
                    SearchResponse response = client().prepareSearch(getDbName()).get();
                    logger.info("  -> got {} docs in {} index", response.getHits().totalHits(), getDbName());
                    return response.getHits().totalHits() == nbDocs;
                } catch (IndexMissingException e) {
                    return false;
                }
            }
        }, 1, TimeUnit.MINUTES), equalTo(true));
    }

    /**
     * Test case for #71: https://github.com/elasticsearch/elasticsearch-river-couchdb/issues/71
     */
    @Test
    public void testDropCouchdbDatabaseWhileRunning_71() throws IOException, InterruptedException {
        final int nbDocs = between(50, 300);
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "couchdb")
                .endObject(), nbDocs, null);

        logger.info("  -> Removing test database [{}]", getDbName());
        CouchDBClient.dropTestDatabase(getDbName());

        // We wait for 10 seconds
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                return false;
            }
        }, 10, TimeUnit.SECONDS);
    }
}
