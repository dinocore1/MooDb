/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.dbi;

/**
 * Used to return the VLSN and LSN for a record.  The VLSN is a unique version
 * for a rep group, and the LSN is unique for a single node.
 */
public class RecordVersion {

    private final long vlsn;
    private final long lsn;

    RecordVersion(long vlsn, long lsn) {
        this.vlsn = vlsn;
        this.lsn = lsn;
    }

    public long getVLSN() {
        return vlsn;
    }

    public long getLSN() {
        return lsn;
    }
}
