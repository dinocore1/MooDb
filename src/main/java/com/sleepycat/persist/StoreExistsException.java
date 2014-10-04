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
 * StoreConfig#setExclusiveCreate ExclusiveCreate} configuration parameter is
 * true and the store's internal catalog database already exists.
 *
 * @author Mark Hayes
 */
public class StoreExistsException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public StoreExistsException(String message) {
        super(message);
    }

    /* <!-- begin JE only --> */

    /** 
     * For internal use only.
     * @hidden 
     */
    private StoreExistsException(String message,
                                 OperationFailureException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new StoreExistsException(msg, this);
    }

    /* <!-- end JE only --> */
}
