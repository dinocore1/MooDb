/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2005-2010 Oracle.  All rights reserved.
 *
 */

package com.sleepycat.je.utilint;

import java.io.File;
import java.util.Map;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Interface for opening a ReplicatedEnvironment from a JE standalone utility,
 * DbCacheSize.  Implemented by com.sleepycat.je.rep.utilint.DbCacheSizeRepEnv,
 * which must be instantiated from standalone JE using Class.forName.
 */
public interface DbCacheSizeRepEnv {
    public Environment open(File envHome,
                            EnvironmentConfig envConfig,
                            Map<String, String> repParams);
}
