/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.dbi;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import com.sleepycat.je.cleaner.OffsetList;
import com.sleepycat.je.utilint.DbLsn;

/*
 * The current set of LSNs of children which are not in-memory but are
 * being accumulated, and will be subsequently sorted and processed.  Once
 * they have been accumulated, they will be sorted, fetched, and returned
 * to the user.
 *
 * Represent this as a map from file number to OffsetList holding LSN
 * offsets.
 */
class LSNAccumulator {
    /* File number -> OffsetList<LSN Offsets> */
    private Map<Long, OffsetList> offsetsByFile;
    private int nTotalEntries;
    private long lsnAccMemoryUsage;
    private SortedLSNTreeWalker parent;

    LSNAccumulator(SortedLSNTreeWalker parent) {
        this.parent = parent;
        init();
    }

    private void init() {
        incInternalMemoryUsage(-lsnAccMemoryUsage);
        offsetsByFile = new TreeMap<Long, OffsetList>();
        nTotalEntries = 0;
        incInternalMemoryUsage(MemoryBudget.TREEMAP_OVERHEAD);
    }

    int getNTotalEntries() {
        return nTotalEntries;
    }

    private void incInternalMemoryUsage(long increment) {
        lsnAccMemoryUsage += increment;
        parent.incInternalMemoryUsage(increment);
    }

    void add(long lsn) {
        long fileNumber = DbLsn.getFileNumber(lsn);
        OffsetList offsetsForFile = offsetsByFile.get(fileNumber);
        if (offsetsForFile == null) {
            offsetsForFile = new OffsetList();
            offsetsByFile.put(fileNumber, offsetsForFile);
            incInternalMemoryUsage(MemoryBudget.TFS_LIST_INITIAL_OVERHEAD);
            incInternalMemoryUsage(MemoryBudget.TREEMAP_ENTRY_OVERHEAD);
        }

        boolean newSegment =
            offsetsForFile.add(DbLsn.getFileOffset(lsn), false);
        if (newSegment) {
            incInternalMemoryUsage(MemoryBudget.TFS_LIST_SEGMENT_OVERHEAD);
        }

        nTotalEntries += 1;
    }

    long[] getAndSortPendingLSNs() {
        long[] currentLSNs = new long[nTotalEntries];
        int curIdx = 0;

        for (Map.Entry<Long, OffsetList> fileEntry :
                 offsetsByFile.entrySet()) {
            long fileNumber = fileEntry.getKey();
            for (long fileOffset : fileEntry.getValue().toArray()) {
                currentLSNs[curIdx] = DbLsn.makeLsn(fileNumber, fileOffset);
                curIdx += 1;
            }
        }

        Arrays.sort(currentLSNs);
        init();
        return currentLSNs;
    }

    boolean isEmpty() {
        return nTotalEntries == 0;
    }

    void clear() {
        offsetsByFile.clear();
        nTotalEntries = 0;
        incInternalMemoryUsage(-lsnAccMemoryUsage);
    }
}
