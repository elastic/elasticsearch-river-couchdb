CouchDB River Plugin for ElasticSearch
==================================

The CouchDB River plugin allows to hook into couchdb `_changes` feed and automatically index it into elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-river-couchdb/1.2.0`.

    -------------------------------------
    | CouchDB Plugin | ElasticSearch    |
    -------------------------------------
    | master         | 0.90 -> master   |
    -------------------------------------
    | 1.2.0          | 0.90             |
    -------------------------------------
    | 1.1.0          | 0.19 -> 0.20     |
    -------------------------------------
    | 1.0.0          | 0.18             |
    -------------------------------------

The CouchDB River allows to automatically index couchdb and make it searchable using the excellent [_changes](http://guide.couchdb.org/draft/notifications.html) stream couchdb provides. Setting it up is as simple as executing the following against elasticsearch:

	curl -XPUT 'localhost:9200/_river/my_db/_meta' -d '{
	    "type" : "couchdb",
	    "couchdb" : {
	        "host" : "localhost",
	        "port" : 5984,
	        "db" : "my_db",
	        "filter" : null
	    },
	    "index" : {
	        "index" : "my_db",
	        "type" : "my_db",
	        "bulk_size" : "100",
	        "bulk_timeout" : "10ms"
	    }
	}'

This call will create a river that uses the **_changes** stream to index all data within couchdb. Moreover, any "future" changes will automatically be indexed as well, making your search index and couchdb synchronized at all times.

The couchdb river is provided as a [plugin](https://github.com/elasticsearch/elasticsearch-river-couchdb) (including explanation on how to install it).

On top of that, in case of a failover, the couchdb river will automatically be started on another elasticsearch node, and continue indexing from the last indexed seq.

Bulking
======

Bulking is automatically done in order to speed up the indexing process. If within the specified **bulk_timeout** more changes are detected, changes will be bulked up to **bulk_size** before they are indexed.

Filtering
======

The `changes` stream allows to provide a filter with parameters that will be used by couchdb to filter the stream of changes. Here is how it can be configured:

	{
	    "couchdb" : {
	        "filter" : "test",
	        "filter_params" : {
	            "param1" : "value1",
	            "param2" : "value2"
	        }
	    }
	}

Script Filters
=========

Filtering can also be performed by providing a script (default to JavaScript) that will further process each changed item within the changes stream. The json provided to the script is under a var called **ctx** with the relevant seq stream change (for example, **ctx.doc** will refer to the document, or **ctx.deleted** is the flag if its deleted or not).

Note, this feature requires the `lang-javascript` plugin.

Any other script language supported by ElasticSearch may be used by setting the `scriptType` parameter to the appropriate value. If unspecified, the default is "js" (javascript). See http://www.elasticsearch.org/guide/reference/modules/scripting.html for details. 

The **ctx.doc** can be changed and its value can will be indexed (assuming its not a deleted change). Also, if **ctx.ignore** is set to true, the change seq will be ignore and not applied.

Other possible values that can be set are **ctx.index** to control the index name to index the doc into, **ctx.type** to control the (mapping) type to index into, **ctx._parent** and **ctx._routing**.

Here is an example setting that adds `field1` with value `value1` to all docs:

	{
	    "type" : "couchdb",
	    "couchdb" : {
	        "script" : "ctx.doc.field1 = 'value1'"
	    }
	}

Basic Authentication
===============

Basic Authentication can be used by passing the **user** and **password** attributes.

	{
	    "type" : "couchdb",
	    "couchdb" : {
	        "user" : "alice",
	        "password" : "secret"
	    }
	}

HTTPS
=====

To use HTTPS, pass the **protocol** field. Most likely, you will also have to change the **port**. If you have unfixable problems with the servers certificates for any reason, you can disable hostname verification by passing **no_verify**.

	{
	    "type" : "couchdb",
	    "couchdb" : {
	        "protocol" : "https",
	        "port" : 443,
	        "no_verify" : "true"
	    }
	}


Ignoring Attachments
====================

You can ignore attachments as provided by couchDb for each document (`_attachments` field).

Here is an example setting that disable *attachments* for all docs:

	{
	  "type":"couchdb",
	  "couchdb": {
	    "ignore_attachments":true
	  }
	}

Note, by default, attachments are not ignored (**false**)


Heartbeat
=========

By default, couchdb river set _changes API heartbeat to `10s`.
Since 1.3.0, an additional option has been added to control the HTTP connection timeout (default to `30s`).
you can control both settings using `heartbeat` and `read_timeout` options:

```sh
curl -XPUT 'localhost:9200/_river/my_db/_meta' -d '{
    "type" : "couchdb",
    "couchdb" : {
        "host" : "localhost",
        "port" : 5984,
        "db" : "my_db",
        "heartbeat" : "5s",
        "read_timeout" : "15s"
    }
}'
```

Starting at a Specific Sequence
==========

The CouchDB river stores the `last_seq` value in a document called `_seq` in the `_river` index. You can use this fact to start or resume rivers at a particular sequence.

To have the CouchDB river start at a particular `last_seq`, create a document with contents like this:

````sh
curl -XPUT 'localhost:9200/_river/my_db/_seq' -d '
{
  "couchdb": {
    "last_seq": "100"
  }
}'
````

where 100 is the sequence number you want the river to start from. Then create the `_meta` document as before. The CouchDB river will startup and read the last sequence value and start indexing from there.

Using CouchDB Views
====================

Since 1.2.0, you can fetch documents from CouchDB views.

You will have to define a view in CouchDB that can be called with the document id fetched from _changes API.
So, CouchDB river will call your view with parameter `key="DOCID"`.

As views return a collection of results (aka rows), each row is indexed with an id like DOCID_seq where seq is the sequence number of each row.
If you get back 3 rows for one single change for document with ID=1234, the river will index 3 documents :

* 1234_1
* 1234_2
* 1234_3
 
To use it, you have to define a view in couchDB. For instance, `_design/myviews/_view/myview` with 

```javascript
function(doc) {
 // YOUR CODE HERE
 emit(doc._id, eval('('+YOURCODEHERE+')') );
}
```

You can use it in your couchDb river as follow :

``` javascript
{
  "type":"couchdb",
  "couchdb": {
    "host":"localhost",
    "port":"5984",
    "db":"mydb",
    "view":"myviews/_view/myview",
    "view_ignore_remove":false
  }
}
```

New options :

* `view` : if not null, couchDB river will not fetch content from `_changes` API but only IDs and then will use the
view to retrieve rows using the ID as a key. By default : null

* `view_ignore_remove` : ask the river to ignore removal of rows if there is less rows after a document update.
By default : false so non existing rows will be removed from elastic search.

For example, with the 3 rows described earlier, if you push a new version of the document 1234 in couchDB with only 2 docs, 

If `view_ignore_remove` is false (default), then

* 1234_1 will be updated
* 1234_2 will be updated
* 1234_3 will be removed

If `view_ignore_remove` is true, then

* 1234_1 will be updated
* 1234_2 will be updated
* 1234_3 will not be updated

Script Views
============

The parameter `script_view` allows to create script filters on the view
results. It's working like the `script` parameter, excepted that:

* you have access to the view result, and not the document,
* you must use `view` instead of `ctx`.

Here is a example which set the field `_parent` to the same value as the field
`field1` of the view:

``` javascript
{
    "type": "couchdb",
    "couchdb": {
        ...
        "view": "myviews/_view/myview",
        "script_view": "view._parent = view.field1"
    }
}
```

License
=======

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2013 Shay Banon and ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
