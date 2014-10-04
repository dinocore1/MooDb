/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import com.sleepycat.je.utilint.TestHook;

/**
 * Specifies the attributes of a DiskOrderedCursor.
 * @since 5.0
 */
public class DiskOrderedCursorConfig implements Cloneable {

    /**
     * Default configuration used if null is passed to methods that create a
     * cursor.
     */
    public static final DiskOrderedCursorConfig DEFAULT =
        new DiskOrderedCursorConfig();

    private boolean keysOnly = false;

    private long lsnBatchSize = Long.MAX_VALUE;

    private long internalMemoryLimit = Long.MAX_VALUE;

    private int queueSize = 1000;

    /*
     * When snapshotting the in-memory INs, this is the maximum time to spend
     * traversing the tree and copying references to the INs.
     */
    private long maxSeedMillisecs = Long.MAX_VALUE;

    /*
     * When snapshotting the in-memory INs, this is the maximum number of INs
     * to use for seeding the Disk Ordered Scan.
     */
    private long maxSeedNodes = Long.MAX_VALUE;

    /* For unit tests. */
    private TestHook maxSeedTestHook = null;

    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public DiskOrderedCursorConfig() {
    }

    /**
     * Specify whether the DiskOrderedCursor should return only the key or key
     * + data.  The default value is false (key + data).  If keyOnly is true,
     * then no duplicate records are returned (i.e. the Cursor only descends to
     * the BIN level). If keysOnly is false, then all records, including
     * duplicates are returned.
     *
     * @param keysOnly If true, return only keys from this cursor.
     *
     * @return this
     */
    public DiskOrderedCursorConfig setKeysOnly(final boolean keysOnly) {
        setKeysOnlyVoid(keysOnly);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setKeysOnlyVoid(final boolean keysOnly) {
        this.keysOnly = keysOnly;
    }

    /**
     * Returns true if the DiskOrderedCursor is configured to return only
     * keys.  Returns false if it is configured to return keys + data.
     *
     * @return true if the DiskOrderedCursor is configured to return keys only.
     */
    public boolean getKeysOnly() {
        return keysOnly;
    }

    /**
     * Set the maximum number of LSNs to gather and sort at any one time.  The
     * default is an unlimited number of LSNs.  Setting this lower causes the
     * DiskOrderedScan to use less memory, but it sorts and processes LSNs more
     * frequently thereby causing slower performance.  Setting this higher will
     * in general improve performance at the expense of memory.  Each LSN uses
     * 16 bytes of memory.
     *
     * @param lsnBatchSize the maximum number of LSNs to accumulate and sort
     * per batch.
     *
     * @return this
     */
    public DiskOrderedCursorConfig setLSNBatchSize(final long lsnBatchSize) {
        setLSNBatchSizeVoid(lsnBatchSize);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setLSNBatchSizeVoid(final long lsnBatchSize) {
        this.lsnBatchSize = lsnBatchSize;
    }

    /**
     * Returns the maximum number of LSNs to be sorted that this
     * DiskOrderedCursor is configured for.
     *
     * @return the maximum number of LSNs to be sorted that this
     * DiskOrderedCursor is configured for.
     */
    public long getLSNBatchSize() {
        return lsnBatchSize;
    }

    /**
     * Set the maximum amount of non JE Cache Memory that the DiskOrderedScan
     * can use at one time.  The default is an unlimited amount of memory.
     * Setting this lower causes the DiskOrderedScan to use less memory, but it
     * sorts and processes LSNs more frequently thereby generally causing slower
     * performance.  Setting this higher will in general improve performance at
     * the expense of memory.
     *
     * @param internalMemoryLimit the maximum number of non JE Cache bytes to
     * use.
     *
     * @return this
     */
    public DiskOrderedCursorConfig
        setInternalMemoryLimit(final long internalMemoryLimit) {
        setInternalMemoryLimitVoid(internalMemoryLimit);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setInternalMemoryLimitVoid(final long internalMemoryLimit) {
        this.internalMemoryLimit = internalMemoryLimit;
    }

    /**
     * Returns the maximum amount of non JE Cache Memory that the
     * DiskOrderedScan can use at one time.
     *
     * @return the maximum amount of non JE Cache Memory that preload can use at
     * one time.
     */
    public long getInternalMemoryLimit() {
        return internalMemoryLimit;
    }

    /**
     * Set the queue size for entries being passed between the
     * DiskOrderedCursor producer thread and the application's consumer
     * thread. If the queue size reaches this number of entries, the producer
     * thread will block until the application thread removes one or more
     * entries (by calling ForwardCursor.getNext().  The default is 1000.
     *
     * @param queueSize the maximum number of entries the queue can hold before
     * the producer thread blocks.
     *
     * @return this
     */
    public DiskOrderedCursorConfig setQueueSize(final int queueSize) {
        setQueueSizeVoid(queueSize);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setQueueSizeVoid(final int queueSize) {
        this.queueSize = queueSize;
    }

    /**
     * Returns the maximum number of entries in the queue before the
     * DiskOrderedCursor producer thread blocks.
     *
     * @return the maximum number of entries in the queue before the
     * DiskOrderedCursor producer thread blocks.
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * Set the maximum time in milliseconds to spend seeding the Disk Ordered
     * Scan.  When a Disk Ordered Scan is initiated, it is first seeded with
     * in-memory internal nodes (INs) from the database tree.  The more INs
     * that can be used for the seeding, the better the concurrent performance
     * will be during the actual scan.  However, during the seeding, a shared
     * latch is placed on the root of the database tree, thereby reducing
     * concurrency for non-read operations on the tree until the seeding is
     * finished.
     *
     * @param maxSeedMillisecs the maximum milliseconds to spend seeding the
     * Disk Ordered Scan.
     *
     * @return this
     */
    public DiskOrderedCursorConfig
        setMaxSeedMillisecs(final long maxSeedMillisecs) {
        setMaxSeedMillisecsVoid(maxSeedMillisecs);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setMaxSeedMillisecsVoid(final long maxSeedMillisecs) {
        this.maxSeedMillisecs = maxSeedMillisecs;
    }

    /**
     * Returns the maximum time (in milliseconds) to spend seeding the Disk
     * Ordered Scan.
     *
     * @return the maximum time (in milliseconds) to spend seeding the Disk
     * Ordered Scan.
     */
    public long getMaxSeedMillisecs() {
        return maxSeedMillisecs;
    }

    /**
     * Set the maximum number of internal nodes to use to seed the Disk Ordered
     * Scan.  When a Disk Ordered Scan is initiated, it is first seeded with
     * in-memory internal nodes (INs) from the database tree.  The more INs
     * that can be used for the seeding, the better the concurrent performance
     * will be during the actual scan.  However, during the seeding, a shared
     * latch is placed on the root of the database tree, thereby reducing
     * concurrency for non-read operations on the tree until the seeding is
     * finished.
     *
     * @param maxSeedNodes the maximum number of internal nodes to use to seed 
     * the Disk Ordered Scan.
     *
     * @return this
     */
    public DiskOrderedCursorConfig
        setMaxSeedNodes(final long maxSeedNodes) {
        setMaxSeedNodesVoid(maxSeedNodes);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setMaxSeedNodesVoid(final long maxSeedNodes) {
        this.maxSeedNodes = maxSeedNodes;
    }

    /**
     * Returns the maximum number of internal nodes to use to seed the Disk
     * Ordered Scan.
     *
     * @return the maximum number of internal nodes to use to seed the Disk
     * Ordered Scan.
     */
    public long getMaxSeedNodes() {
        return maxSeedNodes;
    }

    /**
     * @hidden
     * For Unit Tests.
     */
    public DiskOrderedCursorConfig setMaxSeedTestHook(final TestHook testHook) {
        maxSeedTestHook = testHook;
        return this;
    }

    /**
     * @hidden
     * For Unit Tests.
     */
    public TestHook getMaxSeedTestHook() {
        return maxSeedTestHook;
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public DiskOrderedCursorConfig clone() {
        try {
            return (DiskOrderedCursorConfig) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * Returns the values for each configuration attribute.
     *
     * @return the values for each configuration attribute.
     */
    @Override
    public String toString() {
        return "keysOnly=" + keysOnly +
            "\nlsnBatchSize=" + lsnBatchSize +
            "\ninternalMemoryLimit=" + internalMemoryLimit +
            "\nqueueSize=" + queueSize +
            "\nmaxSeedNodes=" + maxSeedNodes +
            "\nmaxSeedMillisecs=" + maxSeedMillisecs;
    }
}
