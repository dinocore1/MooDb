/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree;

import com.sleepycat.je.dbi.DatabaseId;

/**
 * A class that embodies a reference to a BIN that does not rely on a
 * Java reference to the actual BIN.
 */
public class BINReference {
    protected byte[] idKey;
    private long nodeId;
    private DatabaseId databaseId;

    BINReference(long nodeId, DatabaseId databaseId, byte[] idKey) {
        this.nodeId = nodeId;
        this.databaseId = databaseId;
        this.idKey = idKey;
    }

    public long getNodeId() {
        return nodeId;
    }

    public DatabaseId getDatabaseId() {
        return databaseId;
    }

    public byte[] getKey() {
        return idKey;
    }

    /**
     * Compare two BINReferences.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof BINReference)) {
            return false;
        }

        return ((BINReference) obj).nodeId == nodeId;
    }

    @Override
    public int hashCode() {
        return (int) nodeId;
    }

    @Override
    public String toString() {
        return "idKey=" + Key.getNoFormatString(idKey) +
            " nodeId = " + nodeId +
            " db=" + databaseId;
    }
}
