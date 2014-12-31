Contributing to elasticsearch
=============================

Elasticsearch is an open source project and we love to receive contributions from our community — you! There are many ways to contribute, from writing tutorials or blog posts, improving the documentation, submitting bug reports and feature requests or writing code which can be incorporated into Elasticsearch itself.

Bug reports
-----------

If you think you have found a bug in Elasticsearch, first make sure that you are testing against the [latest version of Elasticsearch](http://www.elasticsearch.org/download/) - your issue may already have been fixed. If not, search our [issues list](https://github.com/elasticsearch/elasticsearch/issues) on GitHub in case a similar issue has already been opened.

It is very helpful if you can prepare a reproduction of the bug. In other words, provide a small test case which we can run to confirm your bug. It makes it easier to find the problem and to fix it. Test cases should be provided as `curl` commands which we can copy and paste into a terminal to run it locally, for example:

```sh
# delete the index
curl -XDELETE localhost:9200/test

# insert a document
curl -XPUT localhost:9200/test/test/1 -d '{
 "title": "test document"
}'

# this should return XXXX but instead returns YYY
curl ....
```

Provide as much information as you can. You may think that the problem lies with your query, when actually it depends on how your data is indexed. The easier it is for us to recreate your problem, the faster it is likely to be fixed.

Feature requests
----------------

If you find yourself wishing for a feature that doesn't exist in Elasticsearch, you are probably not alone. There are bound to be others out there with similar needs. Many of the features that Elasticsearch has today have been added because our users saw the need.
Open an issue on our [issues list](https://github.com/elasticsearch/elasticsearch/issues) on GitHub which describes the feature you would like to see, why you need it, and how it should work.

Contributing code and documentation changes
-------------------------------------------

If you have a bugfix or new feature that you would like to contribute to Elasticsearch, please find or open an issue about it first. Talk about what you would like to do. It may be that somebody is already working on it, or that there are particular issues that you should know about before implementing the change.

We enjoy working with contributors to get their code accepted. There are many approaches to fixing a problem and it is important to find the best approach before writing too much code.

The process for contributing to any of the [Elasticsearch repositories](https://github.com/elasticsearch/) is similar. Details for individual projects can be found below.

### Fork and clone the repository

You will need to fork the main Elasticsearch code or documentation repository and clone it to your local machine. See 
[github help page](https://help.github.com/articles/fork-a-repo) for help.

Further instructions for specific projects are given below.

### Submitting your changes

Once your changes and tests are ready to submit for review:

1. Test your changes
Run the test suite to make sure that nothing is broken.

2. Sign the Contributor License Agreement
Please make sure you have signed our [Contributor License Agreement](http://www.elasticsearch.org/contributor-agreement/). We are not asking you to assign copyright to us, but to give us the right to distribute your code without restriction. We ask this of all contributors in order to assure our users of the origin and continuing existence of the code. You only need to sign the CLA once.

3. Rebase your changes
Update your local repository with the most recent code from the main Elasticsearch repository, and rebase your branch on top of the latest master branch. We prefer your changes to be squashed into a single commit.

4. Submit a pull request
Push your local changes to your forked copy of the repository and [submit a pull request](https://help.github.com/articles/using-pull-requests). In the pull request, describe what your changes do and mention the number of the issue where discussion has taken place, eg "Closes #123".

Then sit back and wait. There will probably be discussion about the pull request and, if any changes are needed, we would love to work with you to get your pull request merged into Elasticsearch.


Contributing to the Elasticsearch plugin
----------------------------------------

**Repository:** [https://github.com/elasticsearch/elasticsearch-river-couchdb](https://github.com/elasticsearch/elasticsearch-river-couchdb)

Make sure you have [Maven](http://maven.apache.org) installed, as Elasticsearch uses it as its build system. Integration with IntelliJ and Eclipse should work out of the box. Eclipse users can automatically configure their IDE by running `mvn eclipse:eclipse` and then importing the project into their workspace: `File > Import > Existing project into workspace`.

Please follow these formatting guidelines:

* Java indent is 4 spaces
* Line width is 140 characters
* The rest is left to Java coding standards
* Disable “auto-format on save” to prevent unnecessary format changes. This makes reviews much harder as it generates unnecessary formatting changes. If your IDE supports formatting only modified chunks that is fine to do.

To create a distribution from the source, simply run:

```sh
cd elasticsearch-river-couchdb/
mvn clean package -DskipTests
```

You will find the newly built packages under: `./target/releases/`.

Before submitting your changes, run the test suite to make sure that nothing is broken, with:

```sh
mvn clean test -Dtests.couchdb=true
```

Source: [Contributing to elasticsearch](http://www.elasticsearch.org/contributing-to-elasticsearch/)
