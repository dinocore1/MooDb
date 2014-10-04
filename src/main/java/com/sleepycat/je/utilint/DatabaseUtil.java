/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.utilint;

import com.sleepycat.je.DatabaseEntry;

/**
 * Utils for use in the db package.
 */
public class DatabaseUtil {

    /**
     * Throw an exception if the parameter is null.
     *
     * @throws IllegalArgumentException via any API method
     */
    public static void checkForNullParam(final Object param,
                                         final String name) {
        if (param == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }

    /**
     * Throw an exception if the parameter is a null or 0-length array.
     *
     * @throws IllegalArgumentException via any API method
     */
    public static void checkForZeroLengthArrayParam(final Object[] param,
                                                    final String name) {
        checkForNullParam(param, name);
        if (param.length == 0) {
            throw new IllegalArgumentException(name + " cannot be zero length");
        }
    }

    /**
     * Throw an exception if the entry is null or the data field is not set.
     *
     * @throws IllegalArgumentException via any API method that takes a
     * required DatabaseEntry param
     */
    public static void checkForNullDbt(final DatabaseEntry entry,
                                       final String name,
                                       final boolean checkData) {
        if (entry == null) {
            throw new IllegalArgumentException
                ("DatabaseEntry " + name + " cannot be null");
        }

        if (checkData) {
            if (entry.getData() == null) {
                throw new IllegalArgumentException
                    ("Data field for DatabaseEntry " +
                     name + " cannot be null");
            }
        }
    }

    /**
     * Throw an exception if the key entry has the partial flag set.  This
     * method should be called for all put() operations.
     *
     * @throws IllegalArgumentException via put methodx
     */
    public static void checkForPartialKey(final DatabaseEntry entry) {
        if (entry.getPartial()) {
            throw new IllegalArgumentException
                ("A partial key DatabaseEntry is not allowed");
        }
    }
}
