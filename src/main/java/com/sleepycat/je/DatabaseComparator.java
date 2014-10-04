/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Implemented by btree and duplicate comparators that need to be initialized
 * before they are used or need access to the environment's ClassLoader
 * property.
 * @since 5.0
 */
public interface DatabaseComparator extends Comparator<byte[]>, Serializable {

    /**
     * Called to initialize a comparator object after it is instantiated or
     * deserialized, and before it is used.
     *
     * @param loader is the environment's ClassLoader property.
     */
    public void initialize(ClassLoader loader);
}
