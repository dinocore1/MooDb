/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.utilint;

import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Used to create running totals across the lifetime of the StatGroup. They
 * cannot be cleared.
 */
public class IntRunningTotalStat extends IntStat {

    private static final long serialVersionUID = 1L;

    public IntRunningTotalStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    @Override
    public void clear() {
       /* Don't clear it because it's a running total. */
    }
}
