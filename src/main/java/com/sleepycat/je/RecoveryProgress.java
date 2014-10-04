/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

/**
 * Describes the different phases of initialization that 
 * be executed when an Environment is instantiated. Meant to be used in
 * conjunction with a {@link ProgressListener} that is configured through
 * {@link EnvironmentConfig#setRecoveryProgressListener} to monitor
 * the cost of environment startup
 * @since 5.0
 */
public enum RecoveryProgress {
        /**
         * Find the last valid entry in the database log.
         */
        FIND_END_OF_LOG, 

        /**
         * Find the last complete checkpoint in the database log.
         */
        FIND_LAST_CKPT, 

        /**
         * Read log entries that pertain to the database map, which is an
         * internal index of all databases.
         */
        READ_DBMAP_INFO, 

        /**
         * Re-read log entries that pertain to the database map when they
         * appear after a btree split.
         */
        REDO_DBMAP_INFO, 

        /**
         * Rollback uncommitted database creation, deletion and truncations.
         */
        UNDO_DBMAP_RECORDS,

        /**
         * Redo committed database creation, deletion and truncations.
         */
        REDO_DBMAP_RECORDS,

        /**
         * Read log entries that pertain to the database indices.
         */
        READ_DATA_INFO,

        /**
         * Re-read log entries that pertain to the database indices when they
         * appear after a btree split.
         */
        REDO_DATA_INFO, 

        /**
         * Rollback uncommitted data operations, such as inserts, updates
         * and deletes.
         */
        UNDO_DATA_RECORDS, 

        /**
         * Repeat committed data operations, such as inserts, updates
         * and deletes.
         */
        REDO_DATA_RECORDS, 

        /**
         * Populate internal metadata which stores information about the
         * utilization level of each log file, for efficient log cleaning.
         */
        POPULATE_UTILIZATION_PROFILE, 

        /**
         * Remove temporary databases created by the application that
         * are no longer valid.
         */
        REMOVE_TEMP_DBS, 

        /**
         * Perform a checkpoint to make all the work of this environment
         * startup persistent, so it is not repeated in future startups.
         */
        CKPT, 

        /** 
         * Basic recovery is completed, and the environment is able to
         * service operations.
         */
        RECOVERY_FINISHED,

        /**
         * For replicated systems only: locate the master of the 
         * replication group by querying others in the group, and holding an
         * election if necessary.
         */
        FIND_MASTER,

        /**
         * For replicated systems only: if a replica, process enough of the
         * replication stream so that the environment fulfills the required
         * consistency policy, as defined by parameters passed to the
         * ReplicatedEnvironment constructor.
         */
        BECOME_CONSISTENT
        }