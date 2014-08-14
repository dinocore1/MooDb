package com.devsmart.moodb;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.sleepycat.je.DatabaseEntry;

import java.io.UnsupportedEncodingException;
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

    public static JsonElement toJsonElement(DatabaseEntry entry, Gson gson) {
        return gson.fromJson(toString(entry), JsonElement.class);
    }

    public static MessageDigest getSha1Hash() {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }


}
