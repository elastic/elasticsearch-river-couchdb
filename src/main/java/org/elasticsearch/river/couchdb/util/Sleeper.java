package org.elasticsearch.river.couchdb.util;

public final class Sleeper {

    private static final long DEFAULT = 1000L;
    private static final long ON_ERROR = 10000L;

    private Sleeper() {}

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {}
    }

    public static void sleep(String why) {
        sleep(DEFAULT);
    }

    public static void sleepLong(String why) {
        sleep(ON_ERROR);
    }
}