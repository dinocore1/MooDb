package com.sleepycat.je.sync.mobile;

import java.util.Collection;

import com.sleepycat.je.sync.SyncDatabase;
import com.sleepycat.je.sync.SyncDataSet;
import com.sleepycat.je.sync.SyncProcessor;

class MobileDataSet extends SyncDataSet {

    MobileDataSet(String dataSetName,
                  SyncProcessor processor,
                  Collection<SyncDatabase> databases) {
        super(dataSetName, processor, databases);
    }
}
