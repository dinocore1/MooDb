/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.utilint;

import static com.sleepycat.je.rep.utilint.SizeAwaitMapStatDefinition.N_NO_WAITS;
import static com.sleepycat.je.rep.utilint.SizeAwaitMapStatDefinition.N_REAL_WAITS;
import static com.sleepycat.je.rep.utilint.SizeAwaitMapStatDefinition.N_WAIT_TIME;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.utilint.RepUtils.ExceptionAwareCountDownLatch;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Creates a Map, that Threads can conveniently wait on to reach a specific
 * size. The wait functionality is provided by the sizeAwait() method
 * defined by this class.
 */
public class SizeAwaitMap<K,V> implements Map<K,V> {
    private EnvironmentImpl envImpl;

    /*
     * The latch map. There is a latch for each threshold of interest to a
     * thread.
     */
    private final 
        HashMap<Integer,ExceptionAwareCountDownLatch> thresholdLatches;

    /* The underlying map of interest to threads. */
    private final Map<K,V> map;

    private final StatGroup stats;
    private final LongStat nNoWaits;
    private final LongStat nRealWaits;
    private final LongStat nWaitTime;

    /**
     * Creates the wrapped Map class. Note that the application must not
     * directly manipulate the underlying map class. The underlying map must
     * be synchronized if it's accessed concurrently.
     *
     * @param map the actual map instance.
     */
    public SizeAwaitMap(EnvironmentImpl envImpl, Map<K, V> map) {
        this.envImpl = envImpl;
        thresholdLatches = new HashMap<Integer,ExceptionAwareCountDownLatch>();
        this.map = map;
        stats = new StatGroup(SizeAwaitMapStatDefinition.GROUP_NAME,
                              SizeAwaitMapStatDefinition.GROUP_DESC);
        nNoWaits = new LongStat(stats, N_NO_WAITS);
        nRealWaits = new LongStat(stats, N_REAL_WAITS);
        nWaitTime = new LongStat(stats, N_WAIT_TIME);
    }

    public StatGroup getStatistics() {
        return stats;
    }

    /**
     * Causes the requesting thread to wait until the map reaches the specified
     * size or the thread is interrupted.
     *
     * @param thresholdSize the size to wait for.
     *
     * @return true if the threshold was reached, false, if the wait timed out.
     *
     * @throws InterruptedException for the usual reasons, or if the map
     * was cleared and the size threshold was not actually reached.
     *
     */
    public boolean sizeAwait(int thresholdSize,
                             long timeout,
                             TimeUnit unit)
        throws InterruptedException {

        assert(thresholdSize >= 0);
        ExceptionAwareCountDownLatch l = null;
        synchronized (this) {
            int size = map.size();
            if (thresholdSize  <= size) {
                nNoWaits.increment();
                return true;
            }
            l = thresholdLatches.get(thresholdSize);
            if (l == null) {
                l = new ExceptionAwareCountDownLatch(envImpl, 1);
                thresholdLatches.put(thresholdSize, l);
            }
        }
        nRealWaits.increment();
        long startTime = System.currentTimeMillis();
        try {
            return l.awaitOrException(timeout, unit);
        } finally {
            nWaitTime.add((System.currentTimeMillis() - startTime));
        }
    }

    /**
     * Used for unit tests only
     * @return
     */
    synchronized int latchCount() {
        return thresholdLatches.size();
    }

    /**
     * Notes the addition of a new value and counts down any latches that were
     * assigned to that threshold.
     */
    synchronized public V put(K key, V value) {
        V oldValue = map.put(key, value);
        if (oldValue == null) {
            /* Incremented size */
            CountDownLatch l = thresholdLatches.remove(map.size());
            if (l != null) {
                l.countDown();
            }
        }
        return oldValue;
    }

    /**
     * It's synchronized so that size() has a stable value in the above
     * methods.
     */
    synchronized public V remove(Object key) {
        return map.remove(key);
    }
    
    /**
     * @deprecated Use {@link #clear(Exception)} instead.
     */
    @Override
    public void clear() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Clears the underlying map and the latch map, after first counting them
     * down, thus permitting them to make progress.
     */
    synchronized public void clear(Exception cause) {
        for (ExceptionAwareCountDownLatch l : thresholdLatches.values()) {
            l.releaseAwait(cause);
        }
        thresholdLatches.clear();
        map.clear();
    }

    /* The remaining methods below merely forward to the underlying map. */

    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return map.containsKey(value);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    public V get(Object key) {
        return map.get(key);
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Set<K> keySet() {
        return map.keySet();
    }

    public void putAll(@SuppressWarnings("unused")
                       Map<? extends K, ? extends V> t) {
        throw EnvironmentFailureException.unexpectedState
            ("putAll not supported");
    }

    public int size() {
        return map.size();
    }

    public Collection<V> values() {
        return map.values();
    }
}
