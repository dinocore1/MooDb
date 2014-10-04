/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.dbi;

/**
 * Used to distinguish Cursor put operations.
 */
public enum PutMode {

    /**
     * User operation: Cursor.putCurrent. Replace data at current position.
     * Return KEYEMPTY if record at current position is deleted.
     */
    CURRENT,

    /**
     * User operation: Cursor.putNoDupData.  Insert duplicate if key/data does
     * not already exist; otherwise, return KEYEXIST.
     */
    NO_DUP_DATA,

    /**
     * User operation: Cursor.putNoOverwrite.  Insert non-duplicate if key does
     * not already exist; otherwise, return KEYEXIST.
     */
    NO_OVERWRITE,

    /**
     * User operation: Cursor.put.  Insert if key (non-duplicate) or key/data
     * (duplicate) does not already exist; otherwise, overwrite data.
     */
    OVERWRITE,
}
