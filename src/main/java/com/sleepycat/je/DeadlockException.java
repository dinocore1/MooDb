/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import com.sleepycat.je.txn.Locker;

/**
 * Thrown when a lock or transaction timeout occurs and {@link
 * EnvironmentConfig#LOCK_OLD_LOCK_EXCEPTIONS} is set to true.
 *
 * <p>Currently (unless {@link EnvironmentConfig#LOCK_OLD_LOCK_EXCEPTIONS} is
 * set to true, see below) {@link DeadlockException} is not thrown by JE
 * because true deadlock detection is not used in JE.  Currently, lock timeouts
 * are used instead, and a deadlock will cause a {@link LockTimeoutException}.
 * When true deadlock detection is added to JE in the future, {@link
 * DeadlockException} will be thrown instead of {@link LockTimeoutException}
 * when a true deadlock occurs.</p>
 *
 * <p>For compatibility with JE 3.3 and earlier, {@link DeadlockException} is
 * thrown instead of {@link LockTimeoutException} and {@link
 * TransactionTimeoutException} when {@link
 * EnvironmentConfig#LOCK_OLD_LOCK_EXCEPTIONS} is set to true.  This
 * configuration parameter is false by default.  See {@link
 * EnvironmentConfig#LOCK_OLD_LOCK_EXCEPTIONS} for information on the changes
 * that should be made to all applications that upgrade from JE 3.3 or
 * earlier.</p>
 *
 * <p>Normally, applications should catch the base class {@link
 * LockConflictException} rather than catching one of its subclasses.  All lock
 * conflicts are typically handled in the same way, which is normally to abort
 * and retry the transaction.  See {@link LockConflictException} for more
 * information.</p>
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception.</p>
 *
 * @deprecated temporarily until true deadlock detection is implemented.
 * Presently, {code DeadlockException} is replaced by {@link
 * LockConflictException} as the common base class for lock conflict
 * exceptions.
 */
public class DeadlockException extends LockConflictException {

    private static final long serialVersionUID = 729943514L;

    /** 
     * For internal use only.
     * @hidden 
     */
    DeadlockException(String message) {
        super(message);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    public DeadlockException(Locker locker, String message) {
        super(locker, message);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    DeadlockException(String message,
                      DeadlockException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new DeadlockException(msg, this);
    }
}
