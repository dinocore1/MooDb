/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.vlsn;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Statistics associated with the VLSN Index used by HA.
 */
public class VLSNIndexStatDefinition {

    public static final String GROUP_NAME = "VLSNIndex";

    public static final String GROUP_DESC = "VLSN Index related stats.";

    public static StatDefinition N_HITS =
        new StatDefinition("nHits",
                           "Number of hits to the VLSN index cache");

    public static StatDefinition N_MISSES =
        new StatDefinition("nMisses",
                           "Number of log entry misses upon access to the " +
                           "VLSN index cache. Upon a miss the Feeder will " +
                           "fetch the log enty from the log buffer, " +
                           "or the log file.");

    public static StatDefinition N_HEAD_BUCKETS_DELETED =
        new StatDefinition("nHeadBucketsDeleted",
                           "Number of VLSN index buckets deleted at the head" +
                           "(the low end) of the VLSN index.");

    public static StatDefinition N_TAIL_BUCKETS_DELETED =
        new StatDefinition("nTailBucketsDeleted",
                           "Number of VLSN index buckets deleted at the tail" +
                           "(the high end) of the index.");

    public static StatDefinition N_BUCKETS_CREATED =
        new StatDefinition("nBucketsCreated",
                           "Number of new VLSN buckets created in the " +
                           "VLSN index.");
}
