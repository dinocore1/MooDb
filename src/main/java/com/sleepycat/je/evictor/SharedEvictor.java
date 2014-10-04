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
 * Create an evictor for a shared cache. [#21330]
 */
public class SharedEvictor extends Evictor {

    public SharedEvictor(EnvironmentImpl envImpl) {
        super(envImpl);
    }

    @Override
    TargetSelector makeSelector() {
        return new SharedSelector(envImpl);
    }

    /**
     * SharedEvictor, unlike PrivateEvictor, must maintain a cache map for each
     * EnvironmentImpl, since each cache map is logically associated with a
     * single DbTree instance.
     */
    @Override
    DbCache createDbCache() {

        return new DbCache() {

            final Map<EnvironmentImpl, Map<DatabaseId, DatabaseImpl>> envMap =
                new HashMap<EnvironmentImpl, Map<DatabaseId, DatabaseImpl>>();

            int nOperations = 0;

            public DatabaseImpl getDb(EnvironmentImpl envImpl,
                                      DatabaseId dbId) {

                Map<DatabaseId, DatabaseImpl> map = envMap.get(envImpl);
                if (map == null) {
                    map = new HashMap<DatabaseId, DatabaseImpl>();
                    envMap.put(envImpl, map);
                }

                /*
                 * Clear DB cache after dbCacheClearCount operations, to
                 * prevent starving other threads that need exclusive access to
                 * the MapLN (for example, DbTree.deleteMapLN).  [#21015]
                 *
                 * Note that we clear the caches for all environments after
                 * dbCacheClearCount total operations, rather than after
                 * dbCacheClearCount operations for a single environment,
                 * because the total is a more accurate representation of
                 * elapsed time, during which other threads may be waiting for
                 * exclusive access to the MapLN.
                 */
                nOperations += 1;
                if ((nOperations % dbCacheClearCount) == 0) {
                    releaseDbs();
                }

                return envImpl.getDbTree().getDb(dbId, -1, map);
            }

            public void releaseDbs() {

                for (Map.Entry<EnvironmentImpl, Map<DatabaseId, DatabaseImpl>>
                     entry : envMap.entrySet()) {

                    final EnvironmentImpl envImpl = entry.getKey();
                    final Map<DatabaseId, DatabaseImpl> map = entry.getValue();

                    envImpl.getDbTree().releaseDbs(map);
                    map.clear();
                }
            }
        };
    }
}
