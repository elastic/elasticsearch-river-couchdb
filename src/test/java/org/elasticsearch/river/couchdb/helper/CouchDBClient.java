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

package org.elasticsearch.river.couchdb.helper;

import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.is;

public class CouchDBClient {
    public static final String host = "localhost";
    public static final Integer port = 5984;
    public static final HttpClient httpClient = new HttpClient(host, port);

    public static void checkCouchDbRunning() {
        HttpClientResponse response = httpClient.request("/");
        Assert.assertThat("Couchdb is not running on [" + host + "][" + port + "]", response.errorCode(), is(200));
    }

    public static void dropTestDatabase(final String dbName) throws InterruptedException {
        // Remove the database
        httpClient.request("DELETE", "/" + dbName);

        // We wait fo the deletion to happen
        ElasticsearchTestCase.awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                HttpClientResponse response = httpClient.request("/" + dbName);
                return response.errorCode() == 404;
            }
        }, 30, TimeUnit.SECONDS);
    }

    public static void createTestDatabase(final String dbName) throws InterruptedException {
        // Create the database
        HttpClientResponse response = httpClient.request("PUT", "/" + dbName);

        Assert.assertThat("can not create database " + dbName, response.errorCode(), is(201));
    }

    public static void dropAndCreateTestDatabase(final String dbName) throws InterruptedException {
        dropTestDatabase(dbName);
        createTestDatabase(dbName);
    }

    public static void putDocument(String dbName, String id, String json) {
        HttpClientResponse response = httpClient.request("PUT", "/" + dbName + "/" + id, json);

        Assert.assertThat("can not create document " + id, response.errorCode(), is(201));
    }

    public static void putDocument(String dbName, String id, String... fielddata) throws IOException {
        XContentBuilder xContentBuilder = jsonBuilder().startObject();
        for (int i = 0; i < fielddata.length; i++) {
            xContentBuilder.field(fielddata[i], fielddata[++i]);
        }
        xContentBuilder.endObject();
        putDocument(dbName, id, xContentBuilder.string());
    }

    public static void putDocumentWithAttachments(String dbName, String id, List<String> doc, String... attachments) throws IOException {
        XContentBuilder xContentBuilder = jsonBuilder()
                .startObject()
                    .field("_id", id);

        if (doc != null) {
            Iterator<String> iterator = doc.iterator();
            while (iterator.hasNext()) {
                xContentBuilder.field(iterator.next(), iterator.next());
            }
        }

        xContentBuilder.startObject("_attachments");

        for (int i = 0; i < attachments.length; i++) {
            String filename = attachments[i];
            byte[] content = attachments[++i].getBytes(StandardCharsets.UTF_8);

            xContentBuilder.startObject(filename)
                        .field("content_type", "text/plain")
                        .field("data", content)
                    .endObject();
        }

         xContentBuilder.endObject().endObject();

        CouchDBClient.putDocument(dbName, id, xContentBuilder.string());
    }

}
