/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * This is a simple test case for testing views.<br>
 * You may have a couchdb instance running on localhost:5984 with a mydb database.<br>
 * You have to define a view named myviews/myview
 * <pre>
function(doc) {
 // YOUR CODE HERE
 emit(doc._id, eval('('+YOURCODEHERE+')') );
}</pre>
 * @author dadoonet (David Pilato)
 */
public class CouchdbRiverViewTest {

    public static void main(String[] args) throws Exception {
    	String host = "localhost";
    	String port = "5984";
    	String db = "mydb";
    	String view = "myviews/_view/myview";
    	boolean viewIgnoreRemove = false;
    	
        Node node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "local")).node();
        Thread.sleep(1000);
        try {
			node.client().admin().indices().delete(new DeleteIndexRequest("_river")).actionGet();
		} catch (IndexMissingException e) {
			// Index does not exist... Fine
		}
        Thread.sleep(1000);
        try {
	        node.client().admin().indices().delete(new DeleteIndexRequest(db)).actionGet();
		} catch (IndexMissingException e) {
			// Index does not exist... Fine
		}
        
        XContentBuilder xb = jsonBuilder()
        		.startObject()
        			.field("type", "couchdb")
        			.startObject("couchdb")
        				.field("host", host)
        				.field("port", port)
        				.field("db", db)
        				.field("view", view)
        				.field("view_ignore_remove", viewIgnoreRemove)
        			.endObject()
        		.endObject();
        node.client().prepareIndex("_river", db, "_meta").setSource(xb).execute().actionGet();

        Thread.sleep(100000);
    }
}

