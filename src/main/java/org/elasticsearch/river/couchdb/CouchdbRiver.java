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

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;
import org.elasticsearch.river.couchdb.kernel.index.Indexer;
import org.elasticsearch.river.couchdb.kernel.slurp.ChangeHandler;
import org.elasticsearch.river.couchdb.kernel.slurp.CouchdbHttpClient;
import org.elasticsearch.river.couchdb.kernel.slurp.LastSeqReader;
import org.elasticsearch.river.couchdb.kernel.slurp.Slurper;
import org.elasticsearch.river.couchdb.kernel.slurp.UrlBuilder;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.common.base.Throwables.propagate;
import static org.elasticsearch.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
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

        String db = databaseConfig.getDatabase();
        LastSeqReader lastSeqReader = new LastSeqReader(databaseConfig, riverConfig, client);
        UrlBuilder urlBuilder = new UrlBuilder(connectionConfig, databaseConfig);
        ChangeHandler changeHandler = new ChangeHandler(db, stream);
        CouchdbHttpClient couchdbHttpClient = new CouchdbHttpClient(null, connectionConfig, changeHandler);
        slurper = new Slurper(db, lastSeqReader, urlBuilder, couchdbHttpClient);

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
}
