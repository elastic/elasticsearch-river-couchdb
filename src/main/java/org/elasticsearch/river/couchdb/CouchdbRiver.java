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

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.base.Throwables.propagate;
import static org.elasticsearch.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.river.couchdb.util.Sleeper.sleepLong;

/**
 *
 */
public class CouchdbRiver extends AbstractRiverComponent implements River {

    private final Client client;

    private final String riverIndexName;

    private final IndexConfig indexConfig;
    private final CouchdbConnectionConfig connectionConfig;
    private final CouchdbDatabaseConfig databaseConfig;
    private final RiverConfig riverConfig;

    private List<Thread> threads = newArrayList();
    private volatile boolean closed;

    private BlockingQueue<String> stream;
    private final ExecutableScript script;

    private Slurper slurper;

    @Inject
    public CouchdbRiver(RiverName riverName, RiverSettings riverSettings, @RiverIndexName String riverIndexName,
                        Client client, ScriptService scriptService) {
        super(riverName, riverSettings);
        this.riverIndexName = riverIndexName;
        this.client = client;

        indexConfig = IndexConfig.fromRiverSettings(riverSettings);
        connectionConfig = CouchdbConnectionConfig.fromRiverSettings(riverSettings);
        databaseConfig = CouchdbDatabaseConfig.fromRiverSettings(riverSettings);
        riverConfig = new RiverConfig(riverName, riverSettings, riverIndexName);
        script = databaseConfig.getScript(scriptService);
    }

    @Override
    public void start() {
        initializeStream();

        initializeIndex();

        ThreadFactory slurperFactory = daemonThreadFactory(settings.globalSettings(), "couchdb_river_slurper");
        ThreadFactory indexerFactory = daemonThreadFactory(settings.globalSettings(), "couchdb_river_indexer");

        LastSeqReader lastSeqReader = new LastSeqReader(databaseConfig, riverConfig, client);
        slurper = new Slurper(connectionConfig, databaseConfig, lastSeqReader, stream);
        threads.add(slurperFactory.newThread(slurper));
        threads.add(indexerFactory.newThread(new Indexer()));

        for (Thread thread : threads) {
            thread.start();
        }
    }

    private void initializeIndex() {
        logger.info("starting couchdb stream: url [{}], database [{}], indexing to [{}]/[{}]",
                connectionConfig.getUrl(), databaseConfig.getDatabase(), indexConfig.getName(), indexConfig.getType());
        int maxAttempts = 10;
        for (int i = 1; i <= maxAttempts; ++i) {
            logger.info("Preparing index=[{}], attempt #{}.", indexConfig.getName(), i);
            try {
                client.admin().indices().prepareCreate(indexConfig.getName()).execute().actionGet();
                return;
            } catch (Exception e) {
                Throwable cause = unwrapCause(e);
                if (cause instanceof IndexAlreadyExistsException) {
                    logger.info("Index=[{}] already exists. No need to create one.", indexConfig.getName());
                    return;
                } else if (cause instanceof ClusterBlockException) {
                    logger.warn("Cluster not recovered yet. Retrying...");
                    sleepLong("to give the cluster some time to recover.");
                } else {
                    logger.warn("failed to create index [{}], disabling river...", e, indexConfig.getName());
                    propagate(e);
                }
            }
        }
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
        if (!closed) {
            logger.info("closing couchdb stream river");
            for (Thread thread : threads) {
                thread.interrupt();
            }
            closed = true;
            slurper.close();
        }
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



}
