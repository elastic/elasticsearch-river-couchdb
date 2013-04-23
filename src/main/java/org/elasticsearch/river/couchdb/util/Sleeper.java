package org.elasticsearch.river.couchdb.util;

public final class Sleeper {

    private static final long NAP_DURATION = 10000L;

    private Sleeper() {}

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {}
    }

    public static void sleep(String why) {
        sleep(NAP_DURATION);
    }
}