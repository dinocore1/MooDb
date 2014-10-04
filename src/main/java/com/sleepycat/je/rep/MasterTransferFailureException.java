/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep;

import com.sleepycat.je.OperationFailureException;

/**
 * Thrown by {@link ReplicatedEnvironment#transferMaster} if a Master Transfer
 * operation cannot be completed within the allotted time.
 */
public class MasterTransferFailureException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public MasterTransferFailureException(String message) {
        super(null /*locker*/, false /*abortOnly*/, message, null /*cause*/);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    private MasterTransferFailureException
        (String message, MasterTransferFailureException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public MasterTransferFailureException wrapSelf(String message) {
        return new MasterTransferFailureException(message, this);
    }
}
