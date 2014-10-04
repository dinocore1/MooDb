/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.dbi;

import com.sleepycat.je.ReplicaConsistencyPolicy;

/**
 * Used to pass a replication configuration instance through the non-HA code.
 */
public interface RepConfigProxy {
    public ReplicaConsistencyPolicy getConsistencyPolicy();
}
