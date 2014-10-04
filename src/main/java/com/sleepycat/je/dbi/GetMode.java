/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.dbi;

/**
 * Internal class used to distinguish which variety of getXXX() that
 * Cursor.retrieveNext should use.
 */
public enum GetMode {
    NEXT("NEXT", true),
    PREV("PREV", false),
    NEXT_DUP("NEXT_DUP", true),
    PREV_DUP("PREV_DUP", false),
    NEXT_NODUP("NEXT_NODUP", true),
    PREV_NODUP("PREV_NODUP", false);

    private String name;
    private boolean forward;

    private GetMode(String name, boolean forward) {
        this.name = name;
        this.forward = forward;
    }

    public final boolean isForward() {
        return forward;
    }

    @Override
    public String toString() {
        return name;
    }
}
