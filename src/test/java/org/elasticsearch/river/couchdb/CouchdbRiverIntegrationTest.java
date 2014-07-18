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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.river.couchdb.helper.CouchDBClient;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.river.couchdb.helper.CouchDBClient.putDocumentWithAttachments;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for CouchDb river<br>
 * You may have a couchdb instance running on localhost:5984 with a mytest database.
 */
@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.SUITE,
        numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
@AbstractCouchdbTest.CouchdbTest
public class CouchdbRiverIntegrationTest extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    private interface InjectorHook {
        public void inject();
    }

    private static final String testDbPrefix = "elasticsearch_couch_test_";

    private String getDbName() {
        return testDbPrefix.concat(Strings.toUnderscoreCase(getTestName()));
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

        if (injectorHook != null) {
            logger.info("  -> Injecting extra data");
            injectorHook.inject();
        }

        logger.info("  -> Create river");
        createIndex(getDbName());
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

}
