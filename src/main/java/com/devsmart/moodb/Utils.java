package com.devsmart.moodb;

import com.devsmart.moodb.objects.DBElement;
import com.devsmart.moodb.objects.JsonElementDBWrapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.sleepycat.je.DatabaseEntry;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {

    static Charset UTF8 = Charset.forName("UTF-8");

    public static byte[] toBytes(String str) {
        return str.getBytes(UTF8);
    }

    public static String toString(byte[] data) {
        return new String(data, UTF8);
    }

    public static String toString(DatabaseEntry entry) {
        String retval = new String(entry.getData(), UTF8);
        return retval;
    }

    public static MessageDigest getSha1Hash() {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static DBElement toDBElement(byte[] data) {
        String jsonStr = toString(data);
        JsonElement element = new JsonParser().parse(jsonStr);
        return JsonElementDBWrapper.wrap(element);
    }

    public static DBElement toDBElement(DatabaseEntry data) {
        String jsonStr = toString(data.getData());
        JsonElement element = new JsonParser().parse(jsonStr);
        return JsonElementDBWrapper.wrap(element);
    }
}
