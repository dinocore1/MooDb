/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree;

public final class Generation {
    static private long nextGeneration = 0;

    static long getNextGeneration() {
        return nextGeneration++;
    }
}
