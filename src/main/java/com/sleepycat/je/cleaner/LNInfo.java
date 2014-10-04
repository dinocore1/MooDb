/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.cleaner;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.tree.LN;

/**
 * The information necessary to lookup an LN.  Used for pending LNs that are
 * locked and must be migrated later, or cannot be migrated immediately during
 * a split.  Also used in a look ahead cache in FileProcessor.
 *
 * Is public for Sizeof only.
 */
public final class LNInfo {

    private LN ln;
    private DatabaseId dbId;
    private byte[] key;

    public LNInfo(LN ln, DatabaseId dbId, byte[] key) {
        this.ln = ln;
        this.dbId = dbId;
        this.key = key;
    }

    LN getLN() {
        return ln;
    }

    DatabaseId getDbId() {
        return dbId;
    }

    byte[] getKey() {
        return key;
    }

    /**
     * Note that the dbId is not counted because it is shared with the
     * DatabaseImpl, where it is accounted for in the memory budget.
     */
    int getMemorySize() {
        int size = MemoryBudget.LN_INFO_OVERHEAD;
        if (ln != null) {
            size += ln.getMemorySizeIncludedByParent();
        }
        if (key != null) {
            size += MemoryBudget.byteArraySize(key.length);
        }
        return size;
    }
}
