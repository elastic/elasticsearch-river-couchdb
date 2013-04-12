package org.elasticsearch.river.couchdb.util;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.couchdb.Slurper;

public class LoggerHelper {

    public static ESLogger slurperLogger(Class<?> clazz, String database) {
        return Loggers.getLogger(clazz, String.format("%s for db=[%s]", Slurper.class.getSimpleName(), database));
    }

}
