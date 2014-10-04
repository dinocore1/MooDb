package com.sleepycat.je.sync;

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.Durability;

/**
 * Optional import parameters that may be passed to a sync operation.
 */
public class ImportConfig {

    public static final ImportConfig DEFAULT = new ImportConfig();

    public ImportConfig setMaxTime(long timeout, TimeUnit unit) {
        return this;
    }

    public long getMaxTime(TimeUnit unit) {
        return 0;
    }

    public ImportConfig setMaxOperations(long writeOperations) {
        return this;
    }

    public long getMaxOperations() {
        return 0;
    }

    public ImportConfig setDurability(Durability durability) {
        return this;
    }

    public Durability getDurability() {
        return null;
    }

    public ImportConfig setRefreshAll(boolean refreshAll) {
        return this;
    }

    public boolean getRefreshAll() {
        return false;
    }
}
