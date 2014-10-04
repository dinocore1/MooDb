/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package com.sleepycat.je.rep;

/**
 * Describes the different phases of replication stream syncup that are
 * executed when a replica starts working with a new replication group master.
 * Meant to be used in conjunction with a 
 * {@link com.sleepycat.je.ProgressListener} that is configured through
 * {@link ReplicationConfig#setSyncupProgressListener}, to monitor the
 * occurrence and cost of replica sync-ups.
 * @see <a href="{@docRoot}/../ReplicationGuide/progoverviewlifecycle.html" 
 * target="_top">Replication Group Life Cycle</a>
 * @since 5.0
 */
public enum SyncupProgress {

    /** 
     * Syncup is starting up. The replica and feeder are searching for the
     * most recent common shared point in the replication stream.
     */
    FIND_MATCHPOINT, 
    
    /**
     * A matchpoint has been found, and the replica is determining whether it
     * has to rollback any uncommitted replicated records applied from the
     * previous master.
     */
    CHECK_FOR_ROLLBACK,
    
    /**
     * The replica is rolling back uncommitted replicated records applied from 
     * the previous master. 
     */
    DO_ROLLBACK,
    
    /** Replication stream syncup has ended. */
    END
}
