/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.utilint;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * A stat that keeps track of latency in milliseconds and presents average,
 * min, max, 95th and 99th percentile values.
 */
public class LatencyStat implements Cloneable {
    
    private static final long serialVersionUID = 1L;

    /* 
     * The maximum tracked latency, in milliseconds, it's also the size of the
     * configurable array which is used to save latencies.
     */
    private final int maxTrackedLatencyMillis;

    /* Configure array used to keep track of the latency. */
    private AtomicIntegerArray histogram;

    private volatile long totalNanos;

    /* The maximum latency, it may exceed the maxTrackedLatencyMillis. */
    private volatile long max;

    /* 
     * The minimum latency, it may exceed the maxTrackedLatencyMillis 
     * as well. 
     */
    private volatile long min = Long.MAX_VALUE;
   
    /* Number of operations whose latency exceed maxTrackedLatencyMillis. */
    private volatile AtomicInteger opsOverflow;

    /* The number of total operations that have been tracked. */
    private volatile AtomicInteger numOps;

    private int saveMin;
    private int saveMax;
    private float saveAvg;
    private int saveNumOps;
    private int save95;
    private int save99;
    private int saveOpsOverflow;

    public LatencyStat(long maxTrackedLatencyMillis) {
        this.maxTrackedLatencyMillis = (int) maxTrackedLatencyMillis;
        clear();
    }

    public synchronized void clear() {

        /* 
         * Reassign rather than clear the histogram, to prevent NPE when
         * concurrent set() and calculate calls are made.
         */
        histogram = new AtomicIntegerArray(maxTrackedLatencyMillis);
        numOps = new AtomicInteger();
        opsOverflow = new AtomicInteger();
        totalNanos = 0;
        max = 0;
        min = Long.MAX_VALUE;
    }

    /**
     * Generated the min, max, avg, 95th and 99th percentile for the collected
     * measurements. Do not clear the measurement collection.
     */
    public Latency calculate() {
        return calculate(false);
    }

    /**
     * Generated the min, max, avg, 95th and 99th percentile for the collected
     * measurements, then clear the measurement collection.
     */
    public Latency calculateAndClear() {
        return calculate(true);
    }

    /**
     * Calculate may be called on a stat that is concurrently updating, so
     * while it has to be thread safe, it's a bit inaccurate when there's
     * concurrent activity. That tradeoff is made in order to avoid the cost of
     * synchronization during the set() method.
     */
    private synchronized Latency calculate(boolean clear) {
        long useTotalNanos = totalNanos;
        int totalOps = numOps.get();
        int nOverflow = opsOverflow.get();
        int minIncludingOverflow = (int) min;
        int maxIncludingOverflow = (int) max;

        if (totalOps == 0) {
            return new Latency(maxTrackedLatencyMillis, 0, 0, 0, 0, 0, 0, 0);
        }

        /* 
         * If the minimum latency is larger than the maxTrackedLatencyMillis, 
         * the 95 and 99 percentile will be -1, and we needn't calculate it.
         */
        int percent95 = -1;
        int percent99 = -1;

        if (minIncludingOverflow < maxTrackedLatencyMillis) {
            int percent95Count = (int) ((totalOps - nOverflow) * .95);
            int percent99Count = (int) ((totalOps - nOverflow) * .99);
            int numSamplesSeen = 0;
            for (int latency = 0; 
                 (latency < histogram.length()) && 
                 (numSamplesSeen < percent99Count); 
                 latency++) {

                final int count = histogram.get(latency);

                if (count == 0) {
                    continue;
                }

                if (numSamplesSeen < percent95Count) {
                    percent95 = latency;
                }
            
                percent99 = latency;
                numSamplesSeen += count;
            }
        }

        float avgMs = ((float) (useTotalNanos * 1e-6)) / totalOps;

        saveMax = maxIncludingOverflow;
        saveMin = minIncludingOverflow;
        saveAvg = avgMs;
        saveNumOps = totalOps;
        save95 = percent95;
        save99 = percent99;
        saveOpsOverflow = nOverflow;
                
        if (clear) {
            clear();
        }

        return new Latency(maxTrackedLatencyMillis, saveMin, saveMax, saveAvg, 
                           saveNumOps, save95, save99, saveOpsOverflow);
    }

    /*
     * Record an operation that had a latency of "nanolatency".   
     */
    public void set(long nanoLatency) {
        set(1, nanoLatency);
    }

    /**
     * Record a set of operations that took "nanolatency" long.
     */
    public void set(int numRecordedOps, long nanoLatency) {
        numOps.addAndGet(numRecordedOps);
        
        /* 
         * Keep a count of latency that is precise enough to record sub
         * millisecond values.
         */
        totalNanos += nanoLatency;

        /* Round the latency to determine where to mark the histogram. */
        long useLatency = nanoLatency/numRecordedOps;
        int millisRounded = (int) ((useLatency + (1000000l / 2)) / 1000000l);

        /* Record this latency. */
        if (millisRounded >= maxTrackedLatencyMillis) {
            opsOverflow.incrementAndGet();
        } else {
            histogram.incrementAndGet(millisRounded);
        }

        /* Update the maximum latency if necessary. */
        if (max < millisRounded) {
            max = millisRounded;
        }

        /* Update the minimum latency if necessary. */
        if (min > millisRounded) {
            min = millisRounded;
        }
    }

    /**
     * Add the measurement in "other" to the measurements held here, in order to
     * generate min, max, avg, 95th, 99th percentile for two Latency Stats.
     */
    public synchronized void rollup(LatencyStat other) {
        if (other == null || other == this) {
            throw new IllegalStateException
                ("Can't rollup a LatencyStat that doesn't have any data");
        }

        if (other.maxTrackedLatencyMillis != maxTrackedLatencyMillis) {
            throw new IllegalStateException
                ("Can't rollup a LatencyStat whose maxTrackedLatencyMillis " +
                 "is different");
        }

        totalNanos += other.totalNanos;
        numOps.addAndGet(other.numOps.get());
        opsOverflow.addAndGet(other.opsOverflow.get());
        if (max < other.max) {
            max = other.max;
        }

        for (int latency = 0; latency < histogram.length(); latency++) {
            histogram.addAndGet(latency, other.histogram.get(latency));
        }
    }

    public boolean isEmpty() {
        return (numOps.get() == 0);
    }

    @Override
    public String toString() {
        Latency results = 
            new Latency(maxTrackedLatencyMillis, saveMin, saveMax, saveAvg, 
                        saveNumOps, save95, save99, saveOpsOverflow);
        return results.toString();
    }                        
}
