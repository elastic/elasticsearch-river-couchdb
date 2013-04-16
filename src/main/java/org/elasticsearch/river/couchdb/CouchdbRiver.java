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
import org.elasticsearch.river.couchdb.kernel.index.ChangeCollector;
import org.elasticsearch.river.couchdb.kernel.index.ChangeProcessor;
import org.elasticsearch.river.couchdb.kernel.index.DefaultOnDeleteHook;
import org.elasticsearch.river.couchdb.kernel.index.DefaultOnIndexHook;
import org.elasticsearch.river.couchdb.kernel.index.DocumentHelper;
import org.elasticsearch.river.couchdb.kernel.index.Indexer;
import org.elasticsearch.river.couchdb.kernel.index.LastSeqFormatter;
import org.elasticsearch.river.couchdb.kernel.index.OnDeleteHook;
import org.elasticsearch.river.couchdb.kernel.index.OnIndexHook;
import org.elasticsearch.river.couchdb.kernel.index.RequestFactory;
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

public class CouchdbRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final ScriptService scriptService;

    private final IndexConfig indexConfig;
    private final CouchdbConnectionConfig connectionConfig;
    private final CouchdbDatabaseConfig databaseConfig;
    private final RiverConfig riverConfig;

    private List<Thread> threads = newArrayList();
    private volatile boolean closed;

    private BlockingQueue<String> stream;
    private Slurper slurper;
    private Indexer indexer;

    @Inject
    public CouchdbRiver(RiverName riverName, RiverSettings riverSettings, @RiverIndexName String riverIndexName,
                        Client client, ScriptService scriptService) {
        super(riverName, riverSettings);
        this.client = client;
        this.scriptService = scriptService;

        indexConfig = IndexConfig.fromRiverSettings(riverSettings);
        connectionConfig = CouchdbConnectionConfig.fromRiverSettings(riverSettings);
        databaseConfig = CouchdbDatabaseConfig.fromRiverSettings(riverSettings);
        riverConfig = new RiverConfig(riverName, riverSettings, riverIndexName);
    }

    @Override
    public void start() {
        initializeStream();

        initializeIndex();

        String db = databaseConfig.getDatabase();
        prepareSlurper(db);
        prepareIndexer(db);

        for (Thread thread : threads) {
            thread.start();
        }
    }

    public void initializeStream() {
        if (indexConfig.getThrottleSize() > 0) {
            stream = new ArrayBlockingQueue<String>(indexConfig.getThrottleSize());
        } else {
            stream = new LinkedTransferQueue<String>();
        }
    }

    private void initializeIndex() {
        logger.info("Initializing index for CouchDB river: url [{}], database [{}], indexing to [{}]/[{}].",
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
                    logger.error("Failed to create index=[{}]. River will be disabled.", e, indexConfig.getName());
                    propagate(e);
                }
            }
        }
    }

    private void prepareSlurper(String db) {
        ChangeHandler changeHandler = new ChangeHandler(db, stream);
        CouchdbHttpClient couchdbHttpClient = new CouchdbHttpClient(null, connectionConfig, changeHandler);
        UrlBuilder urlBuilder = new UrlBuilder(connectionConfig, databaseConfig);
        LastSeqReader lastSeqReader = new LastSeqReader(databaseConfig, riverConfig, client);
        slurper = new Slurper(db, lastSeqReader, urlBuilder, couchdbHttpClient);

        ThreadFactory slurperFactory = daemonThreadFactory(settings.globalSettings(), "couchdb_river_slurper");
        threads.add(slurperFactory.newThread(slurper));
    }

    private void prepareIndexer(String db) {
        RequestFactory requestFactory = new RequestFactory(db, riverConfig);
        DocumentHelper documentHelper = new DocumentHelper(indexConfig);
        OnDeleteHook onDeleteHook = new DefaultOnDeleteHook(db, requestFactory, documentHelper);
        OnIndexHook onIndexHook = new DefaultOnIndexHook(db, requestFactory, documentHelper);
        ExecutableScript script = databaseConfig.getScript(scriptService);
        ChangeProcessor changeProcessor = new ChangeProcessor(db, script, indexConfig, onIndexHook, onDeleteHook);
        ChangeCollector changeCollector = new ChangeCollector(stream, indexConfig, changeProcessor);
        LastSeqFormatter lastSeqFormatter = new LastSeqFormatter(db);
        indexer = new Indexer(db, changeCollector, client, lastSeqFormatter, requestFactory);

        ThreadFactory indexerFactory = daemonThreadFactory(settings.globalSettings(), "couchdb_river_indexer");
        threads.add(indexerFactory.newThread(indexer));
    }

    @Override
    public void close() {
        if (!closed) {
            logger.info("Closing CouchDB river.");
            closed = true;
            slurper.close();
            indexer.close();
            for (Thread thread : threads) {
                thread.interrupt();
            }
        }
    }
}
