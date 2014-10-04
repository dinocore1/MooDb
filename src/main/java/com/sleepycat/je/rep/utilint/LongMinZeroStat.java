/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */


package com.sleepycat.je.rep.utilint;

import com.sleepycat.je.utilint.LongMinStat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;

/**
 * For stats where the min value in the range is zero, so that sums, averages,
 * etc. based on positive ranges just work.
 */
public class LongMinZeroStat extends LongMinStat {

    private static final long serialVersionUID = 1L;

    public LongMinZeroStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    @Override
    public Long get() {
        Long value = super.get();
        return (value == Long.MAX_VALUE) ? 0 : value;
    }
}
