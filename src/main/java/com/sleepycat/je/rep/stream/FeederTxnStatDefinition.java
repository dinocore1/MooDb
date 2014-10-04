/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.stream;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for HA Feeder Transaction statistics.
 */
public class FeederTxnStatDefinition {

    public static final String GROUP_NAME = "FeederTxns";
    public static final String GROUP_DESC = "FeederTxns statistics";

    public static StatDefinition TXNS_ACKED =
        new StatDefinition("txnsAcked", "Number of Transaction ack'd.");

    public static StatDefinition TXNS_NOT_ACKED =
        new StatDefinition("txnsNotAcked",
                           "Number of Transactions not Ack'd.");

    public static StatDefinition TOTAL_TXN_MS =
            new StatDefinition("totalTxnMS",
                               "The total elapsed MS across all txns from " +
                               "transaction start to end.");

    public static StatDefinition ACK_WAIT_MS =
        new StatDefinition("ackWaitMS", "Total MS waited for acks.");
}
