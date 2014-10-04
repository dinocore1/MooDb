/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.utilint;

import com.sleepycat.je.EnvironmentFailureException;

/**
 * A Long array JE stat.
 */
public class LongArrayStat extends Stat<long[]> {
    private static final long serialVersionUID = 1L;

    protected long[] array;

    public LongArrayStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    public LongArrayStat(StatGroup group,
                         StatDefinition definition,
                         long[] array) {
        super(group, definition);
        this.array = array;
    }

    @Override
    public long[] get() {
        return array;
    }

    @Override
    public void set(long[] array) {
        this.array = array;
    }

    @Override
    public void add(Stat<long[]> other) {
        throw EnvironmentFailureException.unexpectedState
            ("LongArrayStat doesn't support the add operation.");
    }

    @Override
    public void clear() {
        if (array != null && array.length > 0) {
            for (int i = 0; i < array.length; i++) {
                array[i] = 0;
            }
        }
    }

    @Override
    public LongArrayStat copy() {
        try {
            LongArrayStat ret = (LongArrayStat) super.clone();
            if (array != null && array.length > 0) {
                long[] newArray = new long[array.length];
                System.arraycopy
                    (array, 0, newArray, array.length - 1, array.length);
                ret.set(newArray);
            }

            return ret;
        } catch (CloneNotSupportedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    @Override
    protected String getFormattedValue() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        if (array != null && array.length > 0) {
            boolean first = true;
            for (int i = 0; i < array.length; i++) {
                if (array[i] > 0) {
                    if (!first) {
                        sb.append("; ");
                    }
                    first = false;
                    sb.append("level ").append(i).append(": count=");
                    sb.append(Stat.FORMAT.format(array[i]));
                }
            }
        }
        sb.append("]");

        return sb.toString();
    }

    @Override
    public boolean isNotSet() {
        if (array == null) {
            return true;
        }

        if (array.length == 0) {
            return true;
        }

        return false;
    }
}
