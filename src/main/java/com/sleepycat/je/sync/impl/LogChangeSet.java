package com.sleepycat.je.sync.impl;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/*
 * Used to track the JE on log data for a SyncDataSet. 
 *
 * The cleaner throttle is determined as the 
 * min(LogChangeSet.nextPosition) for all the SyncDataSets. The 
 * EnvironmentImpl should keep a list of the SyncDataSets.
 */
public class LogChangeSet {
    /* Describe the NULL position for a JE log. */
    public static final long NULL_POSITION = 0;

    /* 
     * Following properties are the lsn numbers for a standalone Environment,
     * VLSN numbers for a replicated Environment.
     */

    /* 
     * The start position of the next sync action. 
     *
     * It's the first entry of the first comitted but not yet acked JE
     * transaction on the log.
     */ 
    private long nextSyncStart;

    /* 
     * The end position of last sync action. 
     *
     * It's the commit entry of the last committed and acked JE transaction on
     * the log.
     */
    private long lastSyncEnd;

    public LogChangeSet() {
        this.nextSyncStart = NULL_POSITION;
        this.lastSyncEnd = NULL_POSITION;
    }

    public LogChangeSet(long nextSyncStart,
                        long lastSyncEnd) {
        this.nextSyncStart = nextSyncStart;
        this.lastSyncEnd = lastSyncEnd;
    }

    public long getNextSyncStart() {
        return nextSyncStart;
    }

    public long getLastSyncEnd() {
        return lastSyncEnd;
    }

    public void setNextSyncStart(long nextSyncStart) {        
        this.nextSyncStart = nextSyncStart;
    }

    public void setLastSyncEnd(long lastSyncEnd) {
        this.lastSyncEnd = lastSyncEnd;
    }

    /* The binding used to write the LogChangeSet information to the SyncDB. */
    public static class LogChangeSetBinding 
            extends TupleBinding<LogChangeSet> {

        @Override
        public LogChangeSet entryToObject(TupleInput input) {
            LogChangeSet changeSet =
                new LogChangeSet(input.readLong(), input.readLong());

            return changeSet;
        }

        @Override
        public void objectToEntry(LogChangeSet changeSet, 
                                  TupleOutput output) {
            output.writeLong(changeSet.getNextSyncStart());
            output.writeLong(changeSet.getLastSyncEnd());
        }
    }
}
