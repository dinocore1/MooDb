/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.txn;

import com.sleepycat.je.utilint.DbLsn;

/**
 * This class is a container to encapsulate a LockGrantType and a WriteLockInfo
 * so that they can both be returned from writeLock.
 */
public class LockResult {
    private LockGrantType grant;
    private WriteLockInfo info;

    /* Made public for unittests */
    public LockResult(LockGrantType grant, WriteLockInfo info) {
        this.grant = grant;
        this.info = info;
    }

    public LockGrantType getLockGrant() {
        return grant;
    }

    public void setAbortLsn(long abortLsn, boolean abortKnownDeleted) {
        setAbortLsn(abortLsn, abortKnownDeleted, false);
    }

    public void setAbortLsn(long abortLsn,
                            boolean abortKnownDeleted,
                            boolean createdThisTxn) {
        /* Do not overwrite abort info if this locker previously . */
        if (info != null &&
            info.getNeverLocked()) {
            if (abortLsn != DbLsn.NULL_LSN) {
                info.setAbortLsn(abortLsn);
                info.setAbortKnownDeleted(abortKnownDeleted);
            }
            info.setCreatedThisTxn(createdThisTxn);
            info.setNeverLocked(false);
        }
    }

    public void copyAbortInfo(WriteLockInfo fromInfo) {
        if (info != null) {
            info.copyAbortInfo(fromInfo);
        }
    }

    public void copyWriteLockInfo(LockResult fromResult) {
        copyWriteLockInfo(fromResult.info);
    }

    /**
     * Used to copy write lock info when an LSN is changed.
     */
    public void copyWriteLockInfo(WriteLockInfo fromInfo) {
        if (fromInfo != null) {
            setAbortLsn(fromInfo.getAbortLsn(),
                        fromInfo.getAbortKnownDeleted(),
                        fromInfo.getCreatedThisTxn());
            copyAbortInfo(fromInfo);
        }
    }

    /**
     * Returns the write lock info for a transactional write lock that was
     * granted.  Null is returned if:
     * - no lock was granted, or
     * - the lock granted is not a write lock, or
     * - the locker was non-transactional.
     */
    public WriteLockInfo getWriteLockInfo() {
        return info;
    }
}
