/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.persist;

import com.sleepycat.je.OperationFailureException;

/**
 * Thrown by the {@link EntityStore} constructor when the {@link
 * StoreConfig#setAllowCreate AllowCreate} configuration parameter is false and
 * the store's internal catalog database does not exist.
 *
 * @author Mark Hayes
 */
public class StoreNotFoundException extends OperationFailureException {

    private static final long serialVersionUID = 1895430616L;

    /** 
     * For internal use only.
     * @hidden 
     */
    public StoreNotFoundException(String message) {
        super(message);
    }

    /* <!-- begin JE only --> */

    /** 
     * For internal use only.
     * @hidden 
     */
    private StoreNotFoundException(String message,
                                   OperationFailureException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new StoreNotFoundException(msg, this);
    }

    /* <!-- end JE only --> */
}
