/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.log.entry;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.tree.BINDelta;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A BINDeltaLogEntry knows how to create a whole BIN from a delta entry.
 */
public class BINDeltaLogEntry extends SingleItemEntry
    implements INContainingEntry {

    /**
     * @param logClass
     */
    public BINDeltaLogEntry(Class<BINDelta> logClass) {
        super(logClass);
    }

    /**
     * Construct a log entry for writing to the log.
     */
    public BINDeltaLogEntry(BINDelta delta) {
        super(LogEntryType.LOG_BIN_DELTA, delta);
    }

    /**
     * Resolve a BINDelta item by fetching the full BIN and merging the delta.
     */
    @Override
    public Object getResolvedItem(DatabaseImpl dbImpl) {
        return getIN(dbImpl);
    }

    /*
     * @see com.sleepycat.je.log.entry.INContainingEntry#getIN()
     */
    public IN getIN(DatabaseImpl dbImpl)
        throws DatabaseException {

        BINDelta delta = (BINDelta) getMainItem();
        return delta.reconstituteBIN(dbImpl);
    }

    /*
     * @see com.sleepycat.je.log.entry.INContainingEntry#getDbId()
     */
    public DatabaseId getDbId() {

        BINDelta delta = (BINDelta) getMainItem();
        return delta.getDbId();        
    }

    // TODO why return NULL_LSN?
    public long getPrevFullLsn() {
        return DbLsn.NULL_LSN;
    }

    public long getPrevDeltaLsn() {

        BINDelta delta = (BINDelta) getMainItem();
        return delta.getPrevDeltaLsn();        
    }
}
