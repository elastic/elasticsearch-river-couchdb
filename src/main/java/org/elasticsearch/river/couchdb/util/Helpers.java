package org.elasticsearch.river.couchdb.util;

import static org.elasticsearch.common.base.Joiner.on;
import static org.elasticsearch.common.base.Throwables.propagate;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

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

    public static String asUrlParam(String key, Object value) {
        return on("=").join(toUtf8(key), toUtf8(value.toString()));
    }

    public static String toUtf8(String string) {
        try {
            return URLEncoder.encode(string, "UTF-8");
        } catch (UnsupportedEncodingException impossibru) {
            throw propagate(impossibru);
        }
    }
}
