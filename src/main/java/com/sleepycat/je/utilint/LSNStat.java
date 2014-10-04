/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.utilint;

/**
 * A long JE stat.
 */
public class LSNStat extends LongStat{
    private static final long serialVersionUID = 1L;

    public LSNStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    public LSNStat(StatGroup group, StatDefinition definition, long counter) {
        super(group, definition);
        this.counter = counter;
    }

    @Override
    protected String getFormattedValue() {
        return DbLsn.getNoFormatString(counter);
    }
}
