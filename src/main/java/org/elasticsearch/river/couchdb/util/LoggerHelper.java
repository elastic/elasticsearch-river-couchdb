package org.elasticsearch.river.couchdb.util;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class LoggerHelper {

    public static ESLogger slurperLogger(Class<?> clazz, String database) {
        return Loggers.getLogger(clazz, String.format("Slurper for db=[%s]", database));
    }

    public static ESLogger indexerLogger(Class<?> clazz, String database) {
        return Loggers.getLogger(clazz, String.format("Indexer for db=[%s]", database));
    }
}
