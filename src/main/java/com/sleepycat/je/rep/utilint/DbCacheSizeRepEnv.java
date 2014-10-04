/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2005-2010 Oracle.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.utilint;

import java.io.File;
import java.util.Map;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;

/**
 * Class for opening a ReplicatedEnvironment from a JE standalone utility,
 * DbCacheSize.  Must be instantiated from standalone JE using Class.forName.
 */
public class DbCacheSizeRepEnv
    implements com.sleepycat.je.utilint.DbCacheSizeRepEnv {

    private static final int START_PORT = 30100;
    private static final int PORT_RANGE = 100;

    public Environment open(File envHome,
                            EnvironmentConfig envConfig,
                            Map<String, String> repParams) {
        final String host = "localhost";
        final FreePortLocator locator = new FreePortLocator
            (host, START_PORT, START_PORT + PORT_RANGE);
        final int port = locator.next();
        final String hostPort = host + ':' + port;
        final ReplicationConfig repConfig = new ReplicationConfig
            ("DbCacheSizeGroup", "DbCacheSizeNode", hostPort);
        repConfig.setHelperHosts(hostPort);
        for (Map.Entry<String, String> entry : repParams.entrySet()) {
            repConfig.setConfigParam(entry.getKey(), entry.getValue());
        }
        return new ReplicatedEnvironment(envHome, repConfig, envConfig);
    }
}
