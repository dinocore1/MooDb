package com.sleepycat.je.sync;

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.Durability;

/**
 * Optional export parameters that may be passed to a sync operation.
 */
public class ExportConfig {

    public static final ExportConfig DEFAULT = new ExportConfig();

    public ExportConfig setMaxTime(long timeout, TimeUnit unit) {
        return this;
    }

    public long getMaxTime(TimeUnit unit) {
        return 0;
    }

    public ExportConfig setMaxOperations(long writeOperations) {
        return this;
    }

    public long getMaxOperations() {
        return 0;
    }

    public ExportConfig setDurability(Durability durability) {
        return this;
    }

    public Durability getDurability() {
        return null;
    }
}
