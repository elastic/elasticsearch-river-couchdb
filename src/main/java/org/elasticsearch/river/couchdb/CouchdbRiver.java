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
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Closeables;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.util.concurrent.jsr166y.TransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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
import java.util.*;
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

    private final String couchProtocol;
    private final String couchHost;
    private final int couchPort;
    private final String couchDb;
    private final String couchFilter;
    private final String couchFilterParamsUrl;
    private final String basicAuth;
    private final boolean noVerify;
    private final String couchView;
    private final boolean couchViewIgnoreRemove;
    private final boolean couchIgnoreAttachments;
    private final TimeValue heartbeat;
    private final TimeValue readTimeout;

    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final int throttleSize;

    private final ExecutableScript script;
    private final ExecutableScript scriptView;

    private volatile Thread slurperThread;
    private volatile Thread indexerThread;
    private volatile boolean closed;

    private final BlockingQueue<String> stream;

    @SuppressWarnings({"unchecked"})
    @Inject
    public CouchdbRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;

        if (settings.settings().containsKey("couchdb")) {
            Map<String, Object> couchSettings = (Map<String, Object>) settings.settings().get("couchdb");
            couchProtocol = XContentMapValues.nodeStringValue(couchSettings.get("protocol"), "http");
            noVerify = XContentMapValues.nodeBooleanValue(couchSettings.get("no_verify"), false);
            couchHost = XContentMapValues.nodeStringValue(couchSettings.get("host"), "localhost");
            couchPort = XContentMapValues.nodeIntegerValue(couchSettings.get("port"), 5984);
            couchDb = XContentMapValues.nodeStringValue(couchSettings.get("db"), riverName.name());
            couchFilter = XContentMapValues.nodeStringValue(couchSettings.get("filter"), null);
            if (couchSettings.containsKey("filter_params")) {
                Map<String, Object> filterParams = (Map<String, Object>) couchSettings.get("filter_params");
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Object> entry : filterParams.entrySet()) {
                    try {
                        sb.append("&").append(URLEncoder.encode(entry.getKey(), "UTF-8")).append("=").append(URLEncoder.encode(entry.getValue().toString(), "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        // should not happen...
                    }
                }
                couchFilterParamsUrl = sb.toString();
            } else {
                couchFilterParamsUrl = null;
            }
            heartbeat = XContentMapValues.nodeTimeValue(couchSettings.get("heartbeat"), TimeValue.timeValueSeconds(10));
            readTimeout = XContentMapValues.nodeTimeValue(couchSettings.get("read_timeout"), TimeValue.timeValueSeconds(heartbeat.getSeconds()*3));
            couchView = XContentMapValues.nodeStringValue(couchSettings.get("view"), null);
            couchViewIgnoreRemove = XContentMapValues.nodeBooleanValue(couchSettings.get("view_ignore_remove"), false);
            couchIgnoreAttachments = XContentMapValues.nodeBooleanValue(couchSettings.get("ignore_attachments"), false);
            if (couchSettings.containsKey("user") && couchSettings.containsKey("password")) {
                String user = couchSettings.get("user").toString();
                String password = couchSettings.get("password").toString();
                basicAuth = "Basic " + Base64.encodeBytes((user + ":" + password).getBytes());
            } else {
                basicAuth = null;
            }

            if (couchSettings.containsKey("script")) {
                String scriptType = "js";
                if(couchSettings.containsKey("scriptType")) {
                    scriptType = couchSettings.get("scriptType").toString();
                }

                script = scriptService.executable(scriptType, couchSettings.get("script").toString(), Maps.newHashMap());
            } else {
                script = null;
            }

            if (couchSettings.containsKey("script_view")) {
                String scriptType = "js";
                if(couchSettings.containsKey("script_view_type")) {
                    scriptType = couchSettings.get("script_view_type").toString();
                }

                scriptView = scriptService.executable(scriptType, couchSettings.get("script_view").toString(), Maps.newHashMap());
            } else {
                scriptView = null;
            }
        } else {
            couchProtocol = "http";
            couchHost = "localhost";
            couchPort = 5984;
            couchDb = "db";
            couchFilter = null;
            couchFilterParamsUrl = null;
            couchView = null;
            couchViewIgnoreRemove = false;
            couchIgnoreAttachments = false;
            heartbeat = TimeValue.timeValueSeconds(10);
            readTimeout = TimeValue.timeValueSeconds(heartbeat.getSeconds()*3);
            noVerify = false;
            basicAuth = null;
            script = null;
            scriptView = null;
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), couchDb);
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), couchDb);
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
            throttleSize = XContentMapValues.nodeIntegerValue(indexSettings.get("throttle_size"), bulkSize * 5);
        } else {
            indexName = couchDb;
            typeName = couchDb;
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
            throttleSize = bulkSize * 5;
        }
        if (throttleSize == -1) {
            stream = new LinkedTransferQueue<String>();
        } else {
            stream = new ArrayBlockingQueue<String>(throttleSize);
        }
    }

    @Override
    public void start() {
        logger.info("starting couchdb stream: host [{}], port [{}], filter [{}], view [{}], db [{}], indexing to [{}]/[{}]", couchHost, couchPort, couchFilter, couchView, couchDb, indexName, typeName);
        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "couchdb_river_slurper").newThread(new Slurper());
        indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "couchdb_river_indexer").newThread(new Indexer());
        indexerThread.start();
        slurperThread.start();
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

    private List<Object> getViewRows(String id)
    {
      String file = new StringBuilder().append("/").append(this.couchDb).append("/_design/").append(this.couchView).append("?key=%22").append(id).append("%22").toString();
      String view = fetchURL(file);

      Map<String, Object> ctx = null;
      try {
        ctx = XContentFactory.xContent(XContentType.JSON).createParser(view).mapAndClose();
      }
      catch (IOException e) {
        this.logger.warn("failed to parse {}", e, new Object[] { view });
      }
      return getListFromJsonNode(ctx, "rows");
    }

    private void doDeleteFromView(List<Object> rows, String index, String type, String id, String routing, BulkRequestBuilder bulk)
    {

      if (!this.couchViewIgnoreRemove)
      {
        long oldSize = 0L;
        try
        {
            PrefixQueryBuilder pqb = QueryBuilders.prefixQuery(
                new StringBuilder().append(type).append("._id").toString(),
                new StringBuilder().append(id).append("_").toString());

          CountRequestBuilder count = this.client.prepareCount(new String[] { index }).setQuery(pqb);
          CountResponse response = count.execute().actionGet();
          oldSize = response.getCount();
        } catch (Exception e) {
          this.logger.warn("failed to execute count", e, new Object[0]);
        }

        if (rows.size() < oldSize) {
          for (int i = rows.size() + 1; i < oldSize + 1L; i++) {
            bulk.add(Requests.deleteRequest(index).type(type).id(new StringBuilder().append(id).append("_").append(i).toString()).routing(routing));
          }
        }
      }
    }

    private void deletingFromView(String index, String type, String id, String routing, BulkRequestBuilder bulk)
    {
      doDeleteFromView(getViewRows(id), index, type, id, routing, bulk);
    }

    private void processingView(String index, String type, String id, String routing, String parent, BulkRequestBuilder bulk)
    {
      List<Object> rows = getViewRows(id);

      doDeleteFromView(rows, index, type, id, routing, bulk);

      int rownum = 1;
      for (Iterator<Object> it = rows.iterator(); it.hasNext(); ) { 
    	  Object object = it.next();
			if ((object instanceof Map)) {
				Map<String, Object> row = (Map<String, Object>) object;
				Map<String, Object> value = getMapFromJsonNode(row, "value");
				if (value != null) {
					if (this.logger.isTraceEnabled()) {
						this.logger.trace("row found {}",
								new Object[] { value });
					}

					if (scriptView != null) {
					    scriptView.setNextVar("view", value);
					    try {
					        scriptView.run();
					        // we need to unwrap the ctx...
					        value = (Map<String, Object>) scriptView.unwrap(value);
					    } catch (Exception e) {
					        logger.warn("failed to scriptView process {}, ignoring", e, value);
					    }
					}

					bulk.add(Requests
							.indexRequest(index)
							.type(type)
							.id(new StringBuilder().append(id).append("_")
									.append(rownum++).toString()).source(value)
							.routing(routing)
							.parent(parent));
				}
			}
		}
    }

    private Map<String, Object> getMapFromJsonNode(Map<String, Object> ctx, String node)
    {
      if (!ctx.containsKey(node)) {
        this.logger.warn(new StringBuilder().append(node).append(" does not exist {}").toString(), new Object[] { ctx });
        return new HashMap<String, Object>();
      }
      Map<String, Object> doc = (Map<String, Object>) ctx.get(node);
      return doc;
    }

    private List<Object> getListFromJsonNode(Map<String, Object> ctx, String node)
    {
      if (!ctx.containsKey(node)) {
        this.logger.warn(new StringBuilder().append(node).append(" does not exist {}").toString(), new Object[] { ctx });
        return new ArrayList<Object>();
      }
      List<Object> doc = (List)ctx.get(node);
      return doc;
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

        id = (ctx.get("id") == null) ? null : ctx.get("id").toString();

        if (ctx.containsKey("ignore") && ctx.get("ignore").equals(Boolean.TRUE)) {
            // ignore dock
        } else if (ctx.containsKey("deleted") && ctx.get("deleted").equals(Boolean.TRUE)) {
            String index = extractIndex(ctx);
            String type = extractType(ctx);
            if (logger.isTraceEnabled()) {
                logger.trace("processing [delete]: [{}]/[{}]/[{}]", index, type, id);
            }
            if (this.couchView == null) {
                bulk.add(deleteRequest(index).type(type).id(id).routing(extractRouting(ctx)).parent(extractParent(ctx)));
            } else {
                deletingFromView(index, type, id, extractRouting(ctx), bulk);
            }
		} else {
			String index = extractIndex(ctx);
			String type = extractType(ctx);
			if (this.couchView == null) {
				if (ctx.containsKey("doc")) {
					Map<String, Object> doc = (Map<String, Object>) ctx.get("doc");

					// Remove _attachment from doc if needed
					if (couchIgnoreAttachments) {
						// no need to log that we removed it, the doc indexed
						// will
						// be shown without it
						doc.remove("_attachments");
					} else {
						// TODO by now, couchDB river does not really store
						// attachments but only attachments meta information
						// So we perhaps need to fully support attachments
					}

					if (logger.isTraceEnabled()) {
						logger.trace(
								"processing [index ]: [{}]/[{}]/[{}], source {}",
								index, type, id, doc);
					}

					bulk.add(indexRequest(index).type(type).id(id).source(doc)
							.routing(extractRouting(ctx))
							.parent(extractParent(ctx)));
				} else {
					logger.warn("ignoring unknown change {}", s);
				}
			} else {
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("processing view [index ]: [{}]/[{}]/[{}], view {}", index, type, id,	this.couchView);
				}

				processingView(index, type, id, extractRouting(ctx), extractParent(ctx), bulk);
			}
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
            type = typeName;
        }
        return type;
    }

    private String extractIndex(Map<String, Object> ctx) {
        String index = (String) ctx.get("_index");
        if (index == null) {
            index = indexName;
        }
        return index;
    }

    private String fetchURL(String file)
    {
      return fetchURL(file, null);
    }

	private String fetchURL(String file, TransferQueue<String> tqueue) {
		StringBuffer sbf = new StringBuffer();

		if (this.logger.isDebugEnabled()) {
			this.logger.debug(
					"using host [{}], port [{}], path [{}]",
					new Object[] { this.couchHost,
							Integer.valueOf(this.couchPort), file });
		}

		HttpURLConnection connection = null;
		InputStream is = null;
		try {
			URL url = new URL("http", this.couchHost, this.couchPort, file);
			connection = (HttpURLConnection) url.openConnection();
			if (this.basicAuth != null) {
				connection.addRequestProperty("Authorization", this.basicAuth);
			}
			connection.setDoInput(true);
			connection.setUseCaches(false);
			is = connection.getInputStream();

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					is, "UTF-8"));
			String line;
			while ((line = reader.readLine()) != null) {
				if (this.closed) {
					String str1 = sbf.toString();
					return str1;
				}
				if (line.length() == 0) {
					this.logger.trace("[couchdb] heartbeat", new Object[0]);
					continue;
				}
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("[couchdb] {}", new Object[] { line });
				}
				if (tqueue != null) {
					tqueue.add(line);
					continue;
				}
				sbf.append(line);
			}
		} catch (Exception e) {
			Closeables.closeQuietly(is);
			if (connection != null) {
				try {
					connection.disconnect();
				} catch (Exception e1) {
				} finally {
					connection = null;
				}
			}
			if (this.closed) {
				return sbf.toString();
			}
			this.logger.warn(
					"failed to read from _changes or view, throttling....", e,
					new Object[0]);
			try {
				Thread.sleep(5000L);
			} catch (InterruptedException e1) {
				if (this.closed) {
					Closeables.closeQuietly(is);
				}
			}
		} finally {
			Closeables.closeQuietly(is);
			if (connection != null) {
				try {
					connection.disconnect();
				} catch (Exception e1) {
				} finally {
					connection = null;
				}
			}
		}

		return sbf.toString();
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
                    while ((s = stream.poll(bulkTimeout.millis(), TimeUnit.MILLISECONDS)) != null) {
                        lineSeq = processLine(s, bulk);
                        if (lineSeq != null) {
                            lastSeq = lineSeq;
                        }

                        if (bulk.numberOfActions() >= bulkSize) {
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

                String file = "/" + couchDb + "/_changes?feed=continuous&include_docs=true&heartbeat=" + heartbeat.getMillis();
                String file = "/" + couchDb + "/_changes?feed=continuous&heartbeat=10000";
                if (CouchdbRiver.this.couchView == null) {
                  file = file + "&include_docs=true";
                }
                
                if (couchFilter != null) {
                    try {
                        file = file + "&filter=" + URLEncoder.encode(couchFilter, "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        // should not happen!
                    }
                    if (couchFilterParamsUrl != null) {
                        file = file + couchFilterParamsUrl;
                    }
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
                    logger.debug("using host [{}], port [{}], path [{}]", couchHost, couchPort, file);
                }

                HttpURLConnection connection = null;
                InputStream is = null;
                try {
                    URL url = new URL(couchProtocol, couchHost, couchPort, file);
                    connection = (HttpURLConnection) url.openConnection();
                    if (basicAuth != null) {
                        connection.addRequestProperty("Authorization", basicAuth);
                    }
                    connection.setDoInput(true);
                    connection.setReadTimeout((int) readTimeout.getMillis());
                    connection.setUseCaches(false);

                    if (noVerify) {
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
                    try {
                        Closeables.close(is, true);
                    } catch (IOException e1) {
                        // Ignore
                    }
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
                    try {
                        Closeables.close(is, true);
                    } catch (IOException e1) {
                        // Ignore
                    }
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
