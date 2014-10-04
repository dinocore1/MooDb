/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree;

import com.sleepycat.je.log.LogContext;

/**
 * Extends LogContext to add fields used by IN.beforeLog and afterLog methods.
 */
public class INLogContext extends LogContext {

    /**
     * Whether a BINDelta may be logged.  A BINDelta is logged rather than a BIN
     * if this field is true and other qualifications are met for a delta.
     * Used by BIN.beforeLog.
     *
     * Set by caller.
     */
    public boolean allowDeltas;

    /**
     * Whether a full BIN may be compressed before being logged. Some callers
     * cannot tolerate compression because they rely on the stability of
     * certain slots or slot indices.
     * Used by BIN.beforeLog.
     *
     * Set by caller.
     */
    public boolean allowCompress;
}
