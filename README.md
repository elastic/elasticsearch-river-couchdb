**Important**: This project has been stopped since elasticsearch 2.0.

----

CouchDB River Plugin for Elasticsearch
==================================

The CouchDB River plugin allows to automatically index couchdb and make it searchable using the excellent 
[_changes](http://guide.couchdb.org/draft/notifications.html) stream couchdb provides.

**Rivers are [deprecated](https://www.elastic.co/blog/deprecating_rivers) and will be removed in the future.**
Have a look at [logstash couchdb changes input](http://www.elastic.co/guide/en/logstash/current/plugins-inputs-couchdb_changes.html).

In order to install the plugin, run: 

```sh
bin/plugin install elasticsearch/elasticsearch-river-couchdb/2.6.0
```

You need to install a version matching your Elasticsearch version:

|       Elasticsearch    |CouchDB River Plugin|                                                             Docs                                                                   |
|------------------------|--------------------|------------------------------------------------------------------------------------------------------------------------------------|
|    master              | Build from source  | See below                                                                                                                          |
|    es-1.x              | Build from source  | [2.7.0-SNAPSHOT](https://github.com/elasticsearch/elasticsearch-river-couchdb/tree/es-1.x/#version-270-snapshot-for-elasticsearch-1x)|
|    es-1.6              |     2.6.0         | [2.6.0](https://github.com/elastic/elasticsearch-river-couchdb/tree/v2.6.0/#version-260-for-elasticsearch-16)                  |
|    es-1.5              |     2.5.0         | [2.5.0](https://github.com/elastic/elasticsearch-river-couchdb/tree/v2.5.0/#version-250-for-elasticsearch-15)                  |
|    es-1.4              |     2.4.2         | [2.4.2](https://github.com/elasticsearch/elasticsearch-river-couchdb/tree/v2.4.2/#version-242-for-elasticsearch-14)                  |
|    es-1.3              |     2.3.0         | [2.3.0](https://github.com/elasticsearch/elasticsearch-river-couchdb/tree/v2.3.0/#version-230-for-elasticsearch-13)                  |
|    es-1.2              |     2.2.0          | [2.2.0](https://github.com/elasticsearch/elasticsearch-river-couchdb/tree/v2.2.0/#couchdb-river-plugin-for-elasticsearch)          |
|    es-1.0              |     2.0.0          | [2.0.0](https://github.com/elasticsearch/elasticsearch-river-couchdb/tree/v2.0.0/#couchdb-river-plugin-for-elasticsearch)          |
|    es-0.90             |     1.3.0          | [1.3.0](https://github.com/elasticsearch/elasticsearch-river-couchdb/tree/v1.3.0/#couchdb-river-plugin-for-elasticsearch)          |

To build a `SNAPSHOT` version, you need to build it with Maven:

```bash
mvn clean install
plugin --install river-couchdb \ 
       --url file:target/releases/elasticsearch-river-couchdb-X.X.X-SNAPSHOT.zip
```

Create river
------------

 Setting it up is as simple as executing the following against elasticsearch:

```sh
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
```

This call will create a river that uses the `_changes` stream to index all data within couchdb. Moreover, any "future" changes will automatically be indexed as well, making your search index and couchdb synchronized at all times.

The couchdb river is provided as a [plugin](https://github.com/elasticsearch/elasticsearch-river-couchdb) (including explanation on how to install it).

On top of that, in case of a failover, the couchdb river will automatically be started on another elasticsearch node, and continue indexing from the last indexed seq.

Bulking
======

Bulking is automatically done in order to speed up the indexing process. If within the specified `bulk_timeout` more changes are detected,
changes will be bulked up to `bulk_size` before they are indexed.

Since 1.3.0, by default, `bulk` size is `100`. A bulk is flushed every `5s`. Number of concurrent requests allowed to be executed is 1.
You can modify those settings within index section:

```javascript
{
    "type" : "couchdb",
    "index" : {
        "index" : "my_index",
        "type" : "my_type",
        "bulk_size" : 1000,
        "flush_interval" : "1s",
        "max_concurrent_bulk" : 3
    }
}
```

Filtering
======

The `changes` stream allows to provide a filter with parameters that will be used by couchdb to filter the stream of changes. Here is how it can be configured:

```javascript
{
    "couchdb" : {
        "filter" : "test",
        "filter_params" : {
            "param1" : "value1",
            "param2" : "value2"
        }
    }
}
```

Script Filters
=========

Filtering can also be performed by providing a script that will further process each changed item 
within the changes stream. The json provided to the script is under a var called **ctx** with the relevant seq stream 
change (for example, **ctx.doc** will refer to the document, or **ctx.deleted** is the flag if its deleted or not).

Any other [script language supported by Elasticsearch](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-plugins.html#scripting) 
may be used by setting the `script_type` parameter to the appropriate value. 

If unspecified, the default is `groovy`. 
See [Scripting documentation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-scripting.html) for details. 

The **ctx.doc** can be changed and its value can will be indexed (assuming its not a deleted change). 
Also, if **ctx.ignore** is set to true, the change seq will be ignore and not applied.

Other possible values that can be set are **ctx.index** to control the index name to index the doc into, **ctx.type** 
to control the (mapping) type to index into, **ctx._parent** and **ctx._routing**.

Here is an example setting that adds `field1` with value `value1` to all docs:

```javascript
{
    "type" : "couchdb",
    "couchdb" : {
        "script" : "ctx.doc.field1 = 'value1'"
    }
}
```

Basic Authentication
===============

Basic Authentication can be used by passing the **user** and **password** attributes.

```javascript
{
    "type" : "couchdb",
    "couchdb" : {
        "user" : "alice",
        "password" : "secret"
    }
}
```

HTTPS
=====

To use HTTPS, pass the **protocol** field. Most likely, you will also have to change the **port**. If you have unfixable problems with the servers certificates for any reason, you can disable hostname verification by passing **no_verify**.

```javascript
{
    "type" : "couchdb",
    "couchdb" : {
        "protocol" : "https",
        "port" : 443,
        "no_verify" : "true"
    }
}
```


Ignoring Attachments
====================

You can ignore attachments as provided by couchDb for each document (`_attachments` field).

Here is an example setting that disable *attachments* for all docs:

```javascript
{
  "type":"couchdb",
  "couchdb": {
    "ignore_attachments":true
  }
}
```


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


Examples
========


Indexing Databases with Multiple Types
-------------------------------------

A common pattern in CouchDB is to have a single database hold documents
of multiple types. Typically each document will have a field containing
the type of the document.

For example, a database of products from Amazon might have a book type:

```json
{
  "_id": 1,
  "type" : "book",
  "author" : "Michael McCandless",
  "title" : "Lucene in Action"
}
```

and a CD type:

```json
{
  "_id": 2,
  "type" : "cd",
  "artist" : "Tool",
  "title" : "Undertow"
}
```

Elasticsearch also supports multiple types in the same index and we need
to tell ES where each type from CouchDB goes. You do this with a river
definition like this:

```json
{
  "type" : "couchdb",
  "couchdb" : {
    "host" : "localhost",
    "port" : 5984,
    "db" : "amazon",
    "script" : "ctx._type = ctx.doc.type"
  },
  "index" : {
    "index" : "amazon"
  }
}
```

Setting  `ctx._type` tells Elasticsearch what type in the index to use.
So if doc.type for a CouchDB changeset is "book" then Elasticsearch will
index it as the "book" type.

The script block can be expanded to handle more complicated cases if
your types don't map one-to-one between the systems.

If you need to also handle deleting documents from the right type in
Elasticsearch, be aware that the above setup requires you to delete
documents from CouchDB in a special way. If you use the `DELETE` `HTTP`
verb with CouchDB you will break the above river as ES will be unable
to determine what type you're trying to delete. Instead you must
preserve some information about the document you deleted by using the
CouchDB bulk document interface.

For example, to delete the above "cd" document, you must post a document
like this to the CouchDB server, replacing the `_rev` field with the
revision of the document you want to delete:

```sh
curl -XPOST http://localhost:5984/amazon/_bulk_docs -d '
{
  "docs" : [
    {
        "_id: 2,
        "_rev" : "rev",
        "_deleted" : true,
        "type" : "cd"
    }
  ]
}'
```

This deletes the document while preserving the type information for ES.
You can extend this technique to store more data in deleted documents
but be aware of the disk space usage.

Indexing parent/child documents
-------------------------------

If you need to index relational documents using the parent/child feature, you could
do it using a script filter.

For example, let's say you have two types of document in CouchDB: Regions and Campuses.

```
// Region 1
{
    "type": "region",
    "name": "bretagne"
}
```

```
// Campus 2
{
    "type": "campus",
    "name": "enib",
    "parent_id": "1"
}
```

You can use the following mapping for `campus`:

```
{
    "campus" : {
        "_parent" : {
            "type" : "region"
        }
    }
}
```

And the launch the river with the following script:

```
{
    "type": "couchdb",
    "couchdb": {
        "script": "ctx._type = ctx.doc.type; if (ctx._type == 'campus') { ctx._parent = ctx.doc.parent_id; }"
    }
}
```

Removing fields using a script
------------------------------

You can remove fields using a script like this:

```
{
    "type": "couchdb",
    "couchdb": {
        "script": "var oblitertron = function(x) { var things = [\"foo\"]; var toberemoved = new java.util.ArrayList(); foreach (i : x.keySet()) { if(things.indexOf(i) == -1) { toberemoved.add(i); } } foreach (i : toberemoved) { x.remove(i); } return x; }; ctx.doc = oblitertron(ctx.doc);"
    }
}
```

A more readable version of the script (with comments) is:

```js
var oblitertron = function (x) {
    // List of fields we want to keep. Others will be removed, such as _id, _rev...
    var things = ["foo"];
    var toberemoved = new java.util.ArrayList();
    foreach(i : x.keySet()) {
        if (things.indexOf(i) == -1) {
            // If we find a field to be removed, we store its name in an array
            toberemoved.add(i);
        }
    }
    // We remove useless fields
    foreach(i : toberemoved) {
        x.remove(i);
    }
    // We return the new document which will be indexed.
    return x;
};
ctx.doc = oblitertron(ctx.doc);
```


Tests
=====

To run couchdb integration tests, you need to have couchdb running on `localhost` using default `5984` port.
Then you can run tests using `tests.couchdb` option:

```sh
mvn clean test -Dtests.couchdb=true
```


License
=======

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
