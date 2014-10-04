/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package com.sleepycat.je.rep;


/**
 * This exception indicates that the application attempted an operation that is
 * not permitted when it is in the <code>Replicator.State.Replica state</code>.
 */
public class ReplicaStateException extends StateChangeException {
    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public ReplicaStateException(String message) {
        super(message, null);
    }

    private ReplicaStateException(String message,
                                  ReplicaStateException cause) {
        super(message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public ReplicaStateException wrapSelf(String msg) {
        return new ReplicaStateException(msg, this);
    }
}
