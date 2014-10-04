/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.latch;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Table of latches by thread for debugging.
 */
class LatchTable {

    private ThreadLocal<Set<Object>> latchesByThread;
            
    LatchTable() {
        latchesByThread = new ThreadLocal<Set<Object>>();
    }

    /**
     * Only call under the assert system. This records latching by thread.
     */
    boolean noteLatch(Object latch) {
        Set<Object> threadLatches = latchesByThread.get();
        if (threadLatches == null) {
            threadLatches = new HashSet<Object>();
            latchesByThread.set(threadLatches);
        }
        threadLatches.add(latch);
        return true;
    }

    /**
     * Only call under the assert system. This records latching by thread.
     * @return true if unnoted successfully.
     */
    boolean unNoteLatch(Object latch, String name) {
        Set<Object> threadLatches = latchesByThread.get();
        if (threadLatches == null) {
            return false;
        } else {
            return threadLatches.remove(latch);
        }
    }

    /**
     * Only call under the assert system. This counts held latches.
     */
    int countLatchesHeld() {
        Set<Object> threadLatches = latchesByThread.get();
        if (threadLatches != null) {
            return threadLatches.size();
        } else {
            return 0;
        }
    }

    String latchesHeldToString() {
        Set<Object> threadLatches = latchesByThread.get();
        StringBuilder sb = new StringBuilder();
        if (threadLatches != null) {
            Iterator<Object> i = threadLatches.iterator();
            while (i.hasNext()) {
                sb.append(i.next()).append('\n');
            }
        }
        return sb.toString();
    }

    void clearNotes() {
        latchesByThread = new ThreadLocal<Set<Object>>();
    }
}
