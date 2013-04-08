/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Closeables;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class CouchdbRiver extends AbstractRiverComponent implements River {

    private final Client client;

    private final String riverIndexName;

    private final IndexConfig indexConfig;
    private final CouchdbConnectionConfig connectionConfig;
    private final CouchdbDatabaseConfig databaseConfig;

    private volatile Thread slurperThread;
    private volatile Thread indexerThread;
    private volatile boolean closed;

    private BlockingQueue<String> stream;
    private final ExecutableScript script;

    @Inject
    public CouchdbRiver(RiverName riverName, RiverSettings riverSettings, @RiverIndexName String riverIndexName,
                        Client client, ScriptService scriptService) {
        super(riverName, riverSettings);
        this.riverIndexName = riverIndexName;
        this.client = client;

        indexConfig = IndexConfig.fromRiverSettings(riverSettings);
        connectionConfig = CouchdbConnectionConfig.fromRiverSettings(riverSettings);
        databaseConfig = CouchdbDatabaseConfig.fromRiverSettings(riverSettings);
        script = databaseConfig.getScript(scriptService);
    }

    @Override
    public void start() {
        initializeStream();

        logger.info("starting couchdb stream: url [{}], database [{}], indexing to [{}]/[{}]",
                connectionConfig.getUrl(), databaseConfig.getDatabase(), indexConfig.getName(), indexConfig.getType());
        try {
            client.admin().indices().prepareCreate(indexConfig.getName()).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexConfig.getName());
                return;
            }
        }

        slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "couchdb_river_slurper").newThread(new Slurper());
        indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "couchdb_river_indexer").newThread(new Indexer());
        indexerThread.start();
        slurperThread.start();
    }

    public void initializeStream() {
        if (indexConfig.getThrottleSize() > 0) {
            stream = new ArrayBlockingQueue<String>(indexConfig.getThrottleSize());
        } else {
            stream = new LinkedTransferQueue<String>();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing couchdb stream river");
        slurperThread.interrupt();
        indexerThread.interrupt();
        closed = true;
    }

    @SuppressWarnings({"unchecked"})
    private Object processLine(String s, BulkRequestBuilder bulk) {
        Map<String, Object> ctx;
        try {
            ctx = XContentFactory.xContent(XContentType.JSON).createParser(s).mapAndClose();
        } catch (IOException e) {
            logger.warn("failed to parse {}", e, s);
            return null;
        }
        if (ctx.containsKey("error")) {
            logger.warn("received error {}", s);
            return null;
        }
        Object seq = ctx.get("seq");
        String id = ctx.get("id").toString();

        // Ignore design documents
        if (id.startsWith("_design/")) {
            if (logger.isTraceEnabled()) {
                logger.trace("ignoring design document {}", id);
            }
            return seq;
        }

        if (script != null) {
            script.setNextVar("ctx", ctx);
            try {
                script.run();
                // we need to unwrap the ctx...
                ctx = (Map<String, Object>) script.unwrap(ctx);
            } catch (Exception e) {
                logger.warn("failed to script process {}, ignoring", e, ctx);
                return seq;
            }
        }

        if (ctx.containsKey("ignore") && ctx.get("ignore").equals(Boolean.TRUE)) {
            // ignore dock
        } else if (ctx.containsKey("deleted") && ctx.get("deleted").equals(Boolean.TRUE)) {
            String index = extractIndex(ctx);
            String type = extractType(ctx);
            if (logger.isTraceEnabled()) {
                logger.trace("processing [delete]: [{}]/[{}]/[{}]", index, type, id);
            }
            bulk.add(deleteRequest(index).type(type).id(id).routing(extractRouting(ctx)).parent(extractParent(ctx)));
        } else if (ctx.containsKey("doc")) {
            String index = extractIndex(ctx);
            String type = extractType(ctx);
            Map<String, Object> doc = (Map<String, Object>) ctx.get("doc");

            // Remove _attachment from doc if needed
            if (databaseConfig.shouldIgnoreAttachments()) {
                // no need to log that we removed it, the doc indexed will be shown without it
                doc.remove("_attachments");
            } else {
                // TODO by now, couchDB river does not really store attachments but only attachments meta infomration
                // So we perhaps need to fully support attachments
            }

            if (logger.isTraceEnabled()) {
                logger.trace("processing [index ]: [{}]/[{}]/[{}], source {}", index, type, id, doc);
            }

            bulk.add(indexRequest(index).type(type).id(id).source(doc).routing(extractRouting(ctx)).parent(extractParent(ctx)));
        } else {
            logger.warn("ignoring unknown change {}", s);
        }
        return seq;
    }

    private String extractParent(Map<String, Object> ctx) {
        return (String) ctx.get("_parent");
    }

    private String extractRouting(Map<String, Object> ctx) {
        return (String) ctx.get("_routing");
    }

    private String extractType(Map<String, Object> ctx) {
        String type = (String) ctx.get("_type");
        if (type == null) {
            type = indexConfig.getType();
        }
        return type;
    }

    private String extractIndex(Map<String, Object> ctx) {
        String index = (String) ctx.get("_index");
        if (index == null) {
            index = indexConfig.getName();
        }
        return index;
    }

    private class Indexer implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (closed) {
                    return;
                }
                String s;
                try {
                    s = stream.take();
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                    continue;
                }
                BulkRequestBuilder bulk = client.prepareBulk();
                Object lastSeq = null;
                Object lineSeq = processLine(s, bulk);
                if (lineSeq != null) {
                    lastSeq = lineSeq;
                }

                // spin a bit to see if we can get some more changes
                try {
                    while ((s = stream.poll(indexConfig.getBulkTimeout().millis(), TimeUnit.MILLISECONDS)) != null) {
                        lineSeq = processLine(s, bulk);
                        if (lineSeq != null) {
                            lastSeq = lineSeq;
                        }

                        if (bulk.numberOfActions() >= indexConfig.getBulkSize()) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                }

                if (lastSeq != null) {
                    try {
                        // we always store it as a string
                        String lastSeqAsString = null;
                        if (lastSeq instanceof List) {
                            // bigcouch uses array for the seq
                            try {
                                XContentBuilder builder = XContentFactory.jsonBuilder();
                                //builder.startObject();
                                builder.startArray();
                                for (Object value : ((List) lastSeq)) {
                                    builder.value(value);
                                }
                                builder.endArray();
                                //builder.endObject();
                                lastSeqAsString = builder.string();
                            } catch (Exception e) {
                                logger.error("failed to convert last_seq to a json string", e);
                            }
                        } else {
                            lastSeqAsString = lastSeq.toString();
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("processing [_seq  ]: [{}]/[{}]/[{}], last_seq [{}]", riverIndexName, riverName.name(), "_seq", lastSeqAsString);
                        }
                        bulk.add(indexRequest(riverIndexName).type(riverName.name()).id("_seq")
                                .source(jsonBuilder().startObject().startObject("couchdb").field("last_seq", lastSeqAsString).endObject().endObject()));
                    } catch (IOException e) {
                        logger.warn("failed to add last_seq entry to bulk indexing");
                    }
                }

                try {
                    BulkResponse response = bulk.execute().actionGet();
                    if (response.hasFailures()) {
                        // TODO write to exception queue?
                        logger.warn("failed to execute" + response.buildFailureMessage());
                    }
                } catch (Exception e) {
                    logger.warn("failed to execute bulk", e);
                }
            }
        }
    }


    private class Slurper implements Runnable {
        @SuppressWarnings({"unchecked"})
        @Override
        public void run() {

            while (true) {
                if (closed) {
                    return;
                }

                String lastSeq = null;
                try {
                    client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
                    GetResponse lastSeqGetResponse = client.prepareGet(riverIndexName, riverName().name(), "_seq").execute().actionGet();
                    if (lastSeqGetResponse.isExists()) {
                        Map<String, Object> couchdbState = (Map<String, Object>) lastSeqGetResponse.getSourceAsMap().get("couchdb");
                        if (couchdbState != null) {
                            lastSeq = couchdbState.get("last_seq").toString(); // we know its always a string
                        }
                    }
                } catch (Exception e) {
                    logger.warn("failed to get last_seq, throttling....", e);
                    try {
                        Thread.sleep(5000);
                        continue;
                    } catch (InterruptedException e1) {
                        if (closed) {
                            return;
                        }
                    }
                }

                String file = "/" + databaseConfig.getDatabase() + "/_changes?feed=continuous&include_docs=true&heartbeat=10000";
                if (databaseConfig.shouldUseFilter()) {
                    file += databaseConfig.buildFilterUrlParams();
                }

                if (lastSeq != null) {
                    try {
                        file = file + "&since=" + URLEncoder.encode(lastSeq, "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        // should not happen, but in any case...
                        file = file + "&since=" + lastSeq;
                    }
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("using url [{}], path [{}]", connectionConfig.getUrl(), file);
                }

                HttpURLConnection connection = null;
                InputStream is = null;
                try {
                    URL url = new URL(connectionConfig.getUrl(), file);
                    connection = (HttpURLConnection) url.openConnection();

                    if (connectionConfig.requiresAuthentication()) {
                        connection.addRequestProperty("Authorization", connectionConfig.getBasicAuthHeader());
                    }
                    connection.setDoInput(true);
                    connection.setUseCaches(false);

                    if (!connectionConfig.shouldVerifyHostname()) {
                        ((HttpsURLConnection) connection).setHostnameVerifier(
                                new HostnameVerifier() {
                                    public boolean verify(String string, SSLSession ssls) {
                                        return true;
                                    }
                                }
                        );
                    }

                    is = connection.getInputStream();

                    final BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (closed) {
                            return;
                        }
                        if (line.length() == 0) {
                            logger.trace("[couchdb] heartbeat");
                            continue;
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("[couchdb] {}", line);
                        }
                        // we put here, so we block if there is no space to add
                        stream.put(line);
                    }
                } catch (Exception e) {
                    Closeables.closeQuietly(is);
                    if (connection != null) {
                        try {
                            connection.disconnect();
                        } catch (Exception e1) {
                            // ignore
                        } finally {
                            connection = null;
                        }
                    }
                    if (closed) {
                        return;
                    }
                    logger.warn("failed to read from _changes, throttling....", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        if (closed) {
                            return;
                        }
                    }
                } finally {
                    Closeables.closeQuietly(is);
                    if (connection != null) {
                        try {
                            connection.disconnect();
                        } catch (Exception e1) {
                            // ignore
                        } finally {
                            connection = null;
                        }
                    }
                }
            }
        }
    }
}
