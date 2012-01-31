CouchDB River Plugin for ElasticSearch
==================================

The CouchDB River plugin allows to hook into couchdb `_changes` feed and automatically index it into elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-river-couchdb/1.0.0`.

    -------------------------------------
    | CouchDB Plugin | ElasticSearch    |
    -------------------------------------
    | master         | master (0.19)    |
    -------------------------------------
    | 1.0.0          | 0.18 -> master   |
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
