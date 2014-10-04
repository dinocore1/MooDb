package com.sleepycat.je.sync;

import com.sleepycat.je.OperationFailureException;

/**
 * Thrown by a sync operation when it is explicitly canceled.
 */
public class SyncCanceledException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public SyncCanceledException(String message) {
        super(null /*locker*/, false /*abortOnly*/, message, null /*cause*/);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    private SyncCanceledException(String message,
                                  SyncCanceledException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new SyncCanceledException(msg, this);
    }
}

