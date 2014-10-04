/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.utilint;

/**
 * A boolean JE stat.
 */
public class BooleanStat extends Stat<Boolean> {
    private static final long serialVersionUID = 1L;

    private Boolean value;

    public BooleanStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    @Override
    public Boolean get() {
        return value;
    }

    @Override
    public void set(Boolean newValue) {
        value = newValue;
    }


    @Override
    public void add(Stat<Boolean> otherStat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        value = false;
    }

    @Override
    protected String getFormattedValue() {
        return value.toString();
    }

    @Override
    public boolean isNotSet() {
        return false; // We can't tell if a boolean is not set.
    }
}
