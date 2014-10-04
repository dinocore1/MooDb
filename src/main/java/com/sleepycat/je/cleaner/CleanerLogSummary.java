/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2010 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.cleaner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.cleaner.UtilizationCalculator.AverageSize;

/**
 * Persistent form of log summary information, which currently is only the
 * utilization correction info maintained by UtilizationCalculator, and is
 * logged as part of the CheckpointEnd log entry.
 */
public class CleanerLogSummary implements Loggable {

    private final List<AverageSize> recentAvgLNSizes;
    private long endFileNumAtLastAdjustment;
    private int initialAdjustments;

    public CleanerLogSummary(List<AverageSize> recentAvgLNSizes,
                             long endFileNumAtLastAdjustment,
                             int initialAdjustments) {
        this.recentAvgLNSizes = recentAvgLNSizes;
        this.endFileNumAtLastAdjustment = endFileNumAtLastAdjustment;
        this.initialAdjustments = initialAdjustments;
    }

    public CleanerLogSummary() {
        recentAvgLNSizes = new ArrayList();
        endFileNumAtLastAdjustment = -1;
        initialAdjustments = 0;
    }

    List<AverageSize> getRecentAvgLNSizes() {
        return recentAvgLNSizes;
    }

    long getEndFileNumAtLastAdjustment() {
        return endFileNumAtLastAdjustment;
    }

    int getInitialAdjustments() {
        return initialAdjustments;
    }

    public int getLogSize() {
        int size = LogUtils.getPackedLongLogSize(endFileNumAtLastAdjustment) +
                   LogUtils.getPackedIntLogSize(initialAdjustments) +
                   LogUtils.getPackedIntLogSize(recentAvgLNSizes.size());
        for (AverageSize avg : recentAvgLNSizes) {
            size += LogUtils.getPackedIntLogSize(avg.size);
            size += LogUtils.getPackedIntLogSize(avg.count);
            size += LogUtils.getPackedIntLogSize(avg.estSize);
            size += LogUtils.getPackedIntLogSize(avg.estCount);
        }
        return size;
    }

    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writePackedLong(logBuffer, endFileNumAtLastAdjustment);
        LogUtils.writePackedInt(logBuffer, initialAdjustments);
        LogUtils.writePackedInt(logBuffer, recentAvgLNSizes.size());
        for (AverageSize avg : recentAvgLNSizes) {
            LogUtils.writePackedInt(logBuffer, avg.size);
            LogUtils.writePackedInt(logBuffer, avg.count);
            LogUtils.writePackedInt(logBuffer, avg.estSize);
            LogUtils.writePackedInt(logBuffer, avg.estCount);
        }
    }

    public void readFromLog(ByteBuffer logBuffer, int entryVersion) {
        endFileNumAtLastAdjustment = LogUtils.readPackedLong(logBuffer);
        initialAdjustments = LogUtils.readPackedInt(logBuffer);
        final int nAvgLNSizes = LogUtils.readPackedInt(logBuffer);
        for (int i = 0; i < nAvgLNSizes; i += 1) {
            final int size = LogUtils.readPackedInt(logBuffer);
            final int count = LogUtils.readPackedInt(logBuffer);
            final int estSize = LogUtils.readPackedInt(logBuffer);
            final int estCount = LogUtils.readPackedInt(logBuffer);
            recentAvgLNSizes.add
                (new AverageSize(size, count, estSize, estCount));
        }
    }

    /** Not used. */
    public long getTransactionId() {
        return 0;
    }

    /** Not used. */
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CleanerLogSummary)) {
            return false;
        }
        final CleanerLogSummary o = (CleanerLogSummary) other;
        if (endFileNumAtLastAdjustment != o.endFileNumAtLastAdjustment) {
            return false;
        }
        if (initialAdjustments != o.initialAdjustments) {
            return false;
        }
        if (!recentAvgLNSizes.equals(o.recentAvgLNSizes)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return (int) (endFileNumAtLastAdjustment + initialAdjustments);
    }

    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<CleanerLogSummary endFileNumAtLastAdjustment=\"0x").
           append(Long.toHexString(endFileNumAtLastAdjustment));
        sb.append("\" initialAdjustments=\"").append(initialAdjustments);
        sb.append("\" recentLNSizesAndCounts=\"");
        for (AverageSize avg : recentAvgLNSizes) {
            sb.append("Cor:");
            sb.append(avg.size);
            sb.append('/');
            sb.append(avg.count);
            sb.append("-Est:");
            sb.append(avg.estSize);
            sb.append('/');
            sb.append(avg.estCount);
            sb.append(' ');
        }
        sb.append("\">");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        dumpLog(sb, false);
        return sb.toString();
    }
}
