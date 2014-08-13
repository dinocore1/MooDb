package com.devsmart.moodb;

import com.sleepycat.je.DatabaseEntry;

import java.io.UnsupportedEncodingException;

public class Utils {


    public static String toString(DatabaseEntry entry) {
        try {
            String retval = new String(entry.getData(), "UTF-8");
            return retval;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
