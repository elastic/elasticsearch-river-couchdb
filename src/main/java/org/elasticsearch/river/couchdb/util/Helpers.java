package org.elasticsearch.river.couchdb.util;

import static org.elasticsearch.common.base.Joiner.on;
import static org.elasticsearch.common.base.Throwables.propagate;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

public class Helpers {

    public static final String UTF_8 = "UTF-8";

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
            return URLEncoder.encode(string, UTF_8);
        } catch (UnsupportedEncodingException impossibru) {
            throw propagate(impossibru);
        }
    }

    public static BufferedReader bufferedUtf8ReaderFor(InputStream is) {
        try {
            return new BufferedReader(new InputStreamReader(is, "UTF-8"));
        } catch (UnsupportedEncodingException impossibru) {
            throw propagate(impossibru);
        }
    }

    public static void closeQuietly(HttpURLConnection connection, Reader reader) {
        if (connection != null) {
            try {
                connection.disconnect();
            } catch (Exception ignored) {}
        }
        try {
            if (reader != null) {
               reader.close();
            }
        } catch (IOException ex) {
            // Ignore
        }
    }

}
