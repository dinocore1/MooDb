/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.txn;

/**
 * LockGrantType is an enumeration of the possible results of a lock attempt.
 */
public enum LockGrantType {

    /**
     * The locker did not previously own a lock on the node, and a new lock has
     * been granted.
     */
    NEW,

    /**
     * The locker did not previously own a lock on the node, and must wait for
     * a new lock because a conflicting lock is held by another locker.
     */
    WAIT_NEW,

    /**
     * The locker previously owned a read lock on the node, and a write lock
     * has been granted by upgrading the lock from read to write.
     */
    PROMOTION,

    /**
     * The locker previously owned a read lock on the node, and must wait for a
     * lock upgrade because a conflicting lock is held by another locker.
     */
    WAIT_PROMOTION,

    /**
     * The locker already owns the requested lock, and no new lock or upgrade
     * is needed.
     */
    EXISTING,

    /**
     * The lock has not been granted because a conflicting lock is held by
     * another locker, and the lock timeout expired or a no-wait lock was
     * requested.
     */
    DENIED,

    /**
     * The lock has not been granted because a conflicting lock is held by
     * another locker, and a RangeRestartException must be thrown.
     */
    WAIT_RESTART,

    /**
     * No lock has been granted because LockType.NONE was requested.
     */
    NONE_NEEDED,

    /**
     * No lock is obtained, but the lock is not owned by any locker.  Used to
     * avoid locking an LSN just prior to logging a node and updating the LSN.
     */
    UNCONTENDED,
}
