package org.elasticsearch.river.couchdb;

import static org.elasticsearch.common.base.Throwables.propagate;
import java.net.MalformedURLException;
import java.net.URL;

public class Helpers {

    public static URL asUrl(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw propagate(e);
        }
    }

    public static String nullToEmpty(String input) {
        return input == null ? "" : input;
    }
}
