/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.evictor;

import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * An evictor for a private cache. [#21330]
 */
public class PrivateEvictor extends Evictor {

    public PrivateEvictor(EnvironmentImpl envImpl) {
        super(envImpl);
    }

    @Override
    TargetSelector makeSelector() {
        return new PrivateSelector(envImpl);
    }

    /**
     * PrivateEvictor implements a DbCache simply using a HashMap, like
     * cleaner.FileProcessor and other components.  (SharedEvictor has a more
     * complex implemention.)
     */
    @Override
    DbCache createDbCache() {

        return new DbCache() {

            final Map<DatabaseId, DatabaseImpl> map =
                new HashMap<DatabaseId, DatabaseImpl>();

            int nOperations = 0;

            public DatabaseImpl getDb(EnvironmentImpl envImpl,
                                      DatabaseId dbId) {

                assert envImpl == PrivateEvictor.this.envImpl;

                /*
                 * Clear DB cache after dbCacheClearCount operations, to
                 * prevent starving other threads that need exclusive access to
                 * the MapLN (for example, DbTree.deleteMapLN).  [#21015]
                 */
                nOperations += 1;
                if ((nOperations % dbCacheClearCount) == 0) {
                    releaseDbs();
                }

                return envImpl.getDbTree().getDb(dbId, -1, map);
            }

            public void releaseDbs() {
                envImpl.getDbTree().releaseDbs(map);
                map.clear();
            }
        };
    }
}
