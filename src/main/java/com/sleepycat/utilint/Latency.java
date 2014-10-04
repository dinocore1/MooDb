/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.utilint;

import java.io.Serializable;
import java.text.DecimalFormat;

/**
 * A struct holding the min, max, avg, 95th, and 99th percentile measurements
 * for the collection of values held in a LatencyStat.
 */
public class Latency implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final DecimalFormat FORMAT = 
        new DecimalFormat("###,###,###,###,###,###,###.##");

    private int maxTrackedLatencyMillis;
    private int min;
    private int max;
    private float avg;
    private int totalOps;
    private int percent95;
    private int percent99;
    private int opsOverflow;

    public Latency(int maxTrackedLatencyMillis,
                   int minMillis,
                   int maxMillis,
                   float avg,
                   int totalOps,
                   int percent95,
                   int percent99,
                   int opsOverflow) {
        this.maxTrackedLatencyMillis = maxTrackedLatencyMillis;
        this.min = minMillis;
        this.max = maxMillis;
        this.avg = avg;
        this.totalOps = totalOps;
        this.percent95 = percent95;
        this.percent99 = percent99;
        this.opsOverflow = opsOverflow;
    }
    
    @Override
    public String toString() {
        if (totalOps == 0) {
            return "No operations";
        }
        
        return "maxTrackedLatencyMillis=" + 
               FORMAT.format(maxTrackedLatencyMillis) +
               " totalOps=" + FORMAT.format(totalOps) + 
               " opsOverflow=" + FORMAT.format(opsOverflow) +
               " min=" + FORMAT.format(min) +
               " max=" + FORMAT.format(max) +
               " avg=" + FORMAT.format(avg) +
               " 95%=" + FORMAT.format(percent95) +
               " 99%=" + FORMAT.format(percent99);
    }

    /**
     * @return the number of operations recorded by this stat.
     */
    public int getTotalOps() {
        return totalOps;
    }

    /**
     * @return the number of operations which exceed the max expected latency
     */
    public int getOpsOverflow() {
        return opsOverflow;
    }

    /**
     * @return the max expected latency for this kind of operation
     */
    public int getMaxTrackedLatencyMillis() {
        return maxTrackedLatencyMillis;
    }

    /**
     * @return the fastest latency tracked
     */
    public int getMin() {
        return min;
    }

    /**
     * @return the slowest latency tracked
     */
    public int getMax() {
        return max;
    }

    /**
     * @return the average latency tracked
     */
    public float getAvg() {
        return avg;
    }

    /**
     * @return the 95th percentile latency tracked by the histogram
     */
    public int get95thPercent() {
        return percent95;
    }

    /**
     * @return the 99th percentile latency tracked by the histogram
     */
    public int get99thPercent() {
        return percent99;
    }

    /** 
     * Add the measurements from "other" and recalculate the min, max, and
     * average values. The 95th and 99th percentile are not recalculated, 
     * because the histogram from LatencyStatis not available, and those values
     * can't be generated.
     */
    public void rollup(Latency other) {
        if (other == null || other.totalOps == 0) {
            throw new IllegalStateException
                ("Can't rollup a Latency that doesn't have any data");
        }

        if (maxTrackedLatencyMillis != other.maxTrackedLatencyMillis) {
            throw new IllegalStateException
                ("Can't rollup a Latency whose maxTrackedLatencyMillis is " +
                 "different");
        }

        if (min > other.min) {
            min = other.min;
        }

        if (max < other.max) {
            max = other.max;
        }

        avg = ((totalOps * avg) + (other.totalOps * other.avg)) / 
              (totalOps + other.totalOps);

        /* Clear out 95th and 99th. They have become invalid. */
        percent95 = 0;
        percent99 = 0;

        totalOps += other.totalOps;
        opsOverflow += other.opsOverflow;
    }
}
