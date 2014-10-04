/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

/**
 * Thrown by {@link ForwardCursor#getNext ForwardCursor.getNext} when a
 * {@link DiskOrderedCursor} producer thread throws an exception.
 * This exception wraps that thrown exception;
 *
 * @since 5.0
 */
public class DiskOrderedCursorProducerException
    extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public DiskOrderedCursorProducerException(String message, Throwable cause) {
        super(null /*locker*/, false /*abortOnly*/, message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new DiskOrderedCursorProducerException(msg, this);
    }
}
