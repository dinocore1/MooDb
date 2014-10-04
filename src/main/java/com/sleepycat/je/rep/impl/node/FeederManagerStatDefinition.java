/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.impl.node;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for HA Replay statistics.
 */
public class FeederManagerStatDefinition {

    public static final String GROUP_NAME = "FeederManager";
    public static final String GROUP_DESC =
        "A feeder is a replication stream connection between a master and " +
        "replica nodes.";

    public static StatDefinition N_FEEDERS_CREATED =
        new StatDefinition("nFeedersCreated",
                           "Number of Feeder threads since this node was " +
                           "started.");

    public static StatDefinition N_FEEDERS_SHUTDOWN =
        new StatDefinition("nFeedersShutdown",
                           "Number of Feeder threads that were shut down, " +
                           "either because this node, or the Replica " +
                           "terminated the connection.");

    public static StatDefinition N_MAX_REPLICA_LAG =
        new StatDefinition("nMaxReplicaLag",
                           "The maximum number of VLSNs by which a replica " +
                           "is lagging.");

    public static StatDefinition N_MAX_REPLICA_LAG_NAME =
        new StatDefinition("nMaxReplicaLagName",
                           "The name of the replica with the maximal lag.");
}
