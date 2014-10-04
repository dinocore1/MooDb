/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.dbi;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.DiskOrderedCursorProducerException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.SortedLSNTreeWalker.TreeNodeProcessor;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * This class implements the DiskOrderedCursor. When an instance is
 * constructed, a Producer Thread is created which runs a SortedLSNTreeWalker
 * against the DiskOrderedCursor's Database.  The callback for the SLTW takes
 * Nodes that are passed to it, filters them appropriately (e.g. if the
 * DiskOrderedCursor is configured for keysOnly, it takes the keys from the
 * nodes, but not the data), and then puts those entries on a BlockingQueue
 * which is shared between the Producer Thread and the application thread.
 * When the application calls getNext(), it simply takes an entry off the queue
 * and hands it to the caller.  The entries on the queue are simple KeyAndData
 * structs which hold byte[]'s for the key (and optional) data.  A special
 * instance of KeyAndData is used to indicate that the cursor scan has
 * finished.
 *
 * Because SLTW doesn't really handle concurrent operations very well,
 * DiskOrderedCursor assume a relatively loose set of consistency guarantees,
 * which are in fact, even looser than dirty-read (READ_UNCOMMITTED) semantics.
 *
 * To wit:
 *
 *  The records returned by the scan correspond to the state of the data set at
 *  the beginning of the scan, plus some but not all changes made by the
 *  application after the start of the scan.  The user should not rely on the
 *  scan returning any changes made after the start of the scan.
 *
 *  As with a READ_UNCOMMITTED scan, changes made by all transactions,
 *  including uncommitted transactions, may be returned by the scan.  Also, a
 *  record returned by the scan is not locked, and may be modified or deleted
 *  by the application after it is returned, including modification or deletion
 *  of the record at the cursor position.
 *
 *  If a transactionally correct data set is required, the application must
 *  ensure that all transactions that write to the key range are committed
 *  before the beginning of the scan, and that during the scan no records in
 *  the key range of the scan are inserted, deleted, or modified.
 *
 * If the cleaner is operating concurrently with the SLTW, then it is possible
 * for a file to be deleted and a not-yet-processed LSN (i.e. one which has not
 * yet been returned to the user) might be pointing to that deleted file.
 * Therefore, we must disable file deletion (but not cleaner operation) during
 * the DOS.
 *
 * Operations on DiskOrderedCursorImpls are re-entrant.
 */
public class DiskOrderedCursorImpl implements EnvConfigObserver {

    /*
     * Simple struct to hold key and data byte arrays being passed through the
     * queue.
     */
    private class KeyAndData {
        final byte[] key;
        final byte[] data;

        /**
         * Creates a marker instance, for END_OF_QUEUE.
         */
        private KeyAndData() {
            this.key = null;
            this.data = null;
        }

        /**
         * Creates a key-only instance.
         */
        private KeyAndData(byte[] key) {
            this.key = key;
            this.data = null;
        }

        /**
         * Creates a key-and-data instance.
         */
        private KeyAndData(byte[] key, LN ln) {
            this.key = key;
            this.data = (ln == null) ? null : ln.getData();
        }

        private byte[] getKey() {
            return key;
        }

        private byte[] getData() {
            return data;
        }
    }

    /*
     * The maximum number of entries that the BlockingQueue will store before
     * blocking the producer thread.
     */
    private int queueSize = 1000;

    /* Queue.offer() timeout in msec. */
    private int offerTimeout;

    /* The maximum number of nanos to spend on the initial snapshot. */
    private long maxSeedNanos = Long.MAX_VALUE;

    /* The maximum number of nodes to use for seeding the initial snapshot. */
    private final long maxSeedNodes;

    /* For unit tests. */
    private TestHook maxSeedTestHook = null;

    /* The special KeyAndData which marks the end of the operation. */
    private final KeyAndData END_OF_QUEUE = new KeyAndData();

    private final DiskOrderedScanProcessor callback;
    private final SortedLSNTreeWalker walker;
    private final BlockingQueue<KeyAndData> queue;
    private final Thread producer;
    private final DatabaseImpl dbImpl;
    private final boolean dups;
    private final boolean keysOnly;
    private final RuntimeException SHUTDOWN_REQUESTED_EXCEPTION =
        new RuntimeException("Producer Thread shutdown requested");

    /* DiskOrderedCursors are initialized as soon as they are created. */
    private boolean closed = false;

    private KeyAndData currentNode = null;

    /**
     * The default SortedLSNTreeWalker starts at a database(s) root(s) and
     * recursively descends down from there.  As it descends, it keeps each
     * of the nodes latched, even during the recursion.  That is fine for (e.g.)
     * preload since concurrent operation is not expected.  For a Disk Ordered
     * Scan, this is not suitable because it leaves too many nodes latched,
     * thereby defeating the purpose of the DOS.
     *
     * Rather than start at the database root, DiskOrderedCursorTreeWalker
     * copies all of the in-memory INs in the database tree and uses those as
     * starting points into the tree.  The callback is still called for each
     * IN/BIN/LN in the tree, but we do not recurse down the tree when we are
     * processing one of the in-memory nodes on that list.
     */
    private class DiskOrderedCursorTreeWalker extends SortedLSNTreeWalker {

        private Set<IN> inList;

        private long seedStartTime = Long.MAX_VALUE;

        private class StopGatheringException extends Exception {
        };

        private final StopGatheringException STOP_GATHERING_EXCEPTION =
            new StopGatheringException();

        DiskOrderedCursorTreeWalker(final DatabaseImpl dbImpl,
                                    final long rootLSN,
                                    final DiskOrderedScanProcessor callback) {
            super(new DatabaseImpl[] { dbImpl },
                  false /*setDbState*/,
                  new long[] { rootLSN },
                  callback,
                  null,  /* savedException */
                  null); /* exception predicate */
        }

        /*
         * Use the set of in-memory INs to supply the set of "roots" (starting
         * points in the tree).
         */
        @Override
        protected void walkInternal()
            throws DatabaseException {

            final LSNAccumulator pendingLSNs = new LSNAccumulator(this);
            inList = gatherInMemoryINs();
            for (IN in : inList) {
                try {
                    in.latchShared(CacheMode.UNCHANGED);
                    accumulateLSNs(in, pendingLSNs);
                } finally {
                    in.releaseLatch();
                }
            }

            processAccumulatedLSNs(pendingLSNs);
        }

        /*
         * Only process this child node if it's not on the in-memory IN list.
         * If it is on that list, the callback will be called when that node
         * is actually processed as a parent (i.e. not when it is a child).
         */
        @Override
        protected void processResidentChild(long lsn,
                                            Node node,
                                            byte[] lnKey,
                                            LSNAccumulator pendingLSNs) {

            /*
             * Don't process this child if it's on the inList since it will
             * get passed to us on a different call (either before or after
             * this one).  If keysOnly, then we need to process BINs.
             */
            if (inList.contains(node) &&
                !(keysOnly && node instanceof BIN)) {
                return;
            }

            super.processResidentChild(lsn, node, lnKey, pendingLSNs);
        }

        /*
         * Create a set of all INs that are currently in memory by performing a
         * breadth-first traversal on the tree.  We could use the INList but
         * latching that while we make the copy would have a detrimental
         * impact.
         */
        private Set<IN> gatherInMemoryINs() {
            final Set<IN> ret = new HashSet<IN>();
            final IN root = getOrFetchRootIN(dbImpl,
                                             dbImpl.getTree().getRootLsn());
            if (root == null) {
                return ret;
            }
            try {
                seedStartTime = System.nanoTime();
                try {
                    gatherInMemoryINs1(root, ret);
                } catch (StopGatheringException RT) {
                    /* Do nothing.  Just stop. */
                }
            } finally {
                releaseRootIN(root);
            }
            return ret;
        }

        /**
         * The IN param must be latched on entry and is left latched on return.
         */
        private void gatherInMemoryINs1(final IN in, final Set<IN> ins)
            throws StopGatheringException {

            ins.add(in);

            /*
             * Check if we've exceed the max time that the user wants us to
             * spend on seeding the DOS.
             *
             * Although the TestHookExecute should fall inside the check
             * against maxSeedNanos, this has proven to be unreliable for test
             * purposes because it is hard to find the number of records which
             * (1) uses up > 1 ms (the minimum timeout), and (2) doesn't
             * overflow heap for the unit tests.  So instead, just check if the
             * hook is set, and pass along the amount of time that has passed.
             * Not great, but sufficient for testing the code path.
             */
            long elapsedTime = System.nanoTime() - seedStartTime;
            assert TestHookExecute.doHookIfSet
                (maxSeedTestHook, (elapsedTime > 0 ? 1 : 0));
            if (elapsedTime > maxSeedNanos) {
                throw STOP_GATHERING_EXCEPTION;
            }

            /*
             * Check if we've exceed the max # nodes that the user wants us to
             * spend on seeding the DOS.
             */
            if ((ins.size() > maxSeedNodes)) {
                assert TestHookExecute.doHookIfSet
                    (maxSeedTestHook, ins.size());
                throw STOP_GATHERING_EXCEPTION;
            }

            for (int i = 0; i < in.getNEntries(); i += 1) {
                if (in.isEntryPendingDeleted(i) ||
                    in.isEntryKnownDeleted(i)) {
                    continue;
                }

                final Node node = in.getTarget(i);
                if (node != null && node.isIN()) {
                    final IN child = (IN) node;
                    child.latchShared(CacheMode.UNCHANGED);
                    try {
                        gatherInMemoryINs1(child, ins);
                    } finally {
                        child.releaseLatch();
                    }
                }
            }
        }
    }

    public DiskOrderedCursorImpl(final DatabaseImpl dbImpl,
                                 final DiskOrderedCursorConfig config)
        throws DatabaseException {

        this.dbImpl = dbImpl;
        this.dups = dbImpl.getSortedDuplicates();

        DbConfigManager configMgr =
            dbImpl.getDbEnvironment().getConfigManager();

        offerTimeout =
            configMgr.getDuration(EnvironmentParams.DOS_PRODUCER_QUEUE_TIMEOUT);

        long configMSM = config.getMaxSeedMillisecs();
        if (configMSM != Long.MAX_VALUE) {
            maxSeedNanos = configMSM * 1000000;
        }
        maxSeedNodes = config.getMaxSeedNodes();

        this.keysOnly = config.getKeysOnly();
        this.queueSize = config.getQueueSize();
        final long rootLSN = dbImpl.getTree().getRootLsn();
        this.callback = makeDiskOrderedScanProcessor(this.keysOnly);
        this.walker =
            new DiskOrderedCursorTreeWalker(dbImpl, rootLSN, callback);
        this.walker.setLSNBatchSize(config.getLSNBatchSize());
        this.walker.setInternalMemoryLimit(config.getInternalMemoryLimit());
        this.walker.accumulateLNs = !keysOnly;
        this.maxSeedTestHook = config.getMaxSeedTestHook();
        this.queue = new ArrayBlockingQueue<KeyAndData>(queueSize);
        this.producer = new Thread() {
                public void run() {
                    try {
                        /* Prevent files from being deleted during scan. */
                        dbImpl.getDbEnvironment().getCleaner().
                            addProtectedFileRange(0L);
                        walker.walk();
                        callback.close();
                    } catch (Throwable T) {
                        if (T == SHUTDOWN_REQUESTED_EXCEPTION) {
                            /* Shutdown was requested.  Don't rethrow. */
                            return;
                        }

                        callback.setException(T);
                        queue.offer(END_OF_QUEUE);
                    } finally {
                        /* Allow files to be deleted again. */
                        dbImpl.getDbEnvironment().getCleaner().
                            removeProtectedFileRange(0L);
                    }
                }
            };

        this.producer.setName("DiskOrderedScan Producer Thread for " +
                              Thread.currentThread());
        this.producer.start();
    }

    private DiskOrderedScanProcessor
        makeDiskOrderedScanProcessor(boolean keysOnly) {

        if (keysOnly) {
            return new DiskOrderedScanProcessor() {

                /*
                 * If this is a keysOnly processor, we only want BINs to be
                 * processed.  We get the keys out of them.
                 */
                public void processLSN(@SuppressWarnings("unused")
                                       long childLsn,
                                       LogEntryType childType,
                                       Node node,
                                       @SuppressWarnings("unused")
                                       byte[] ignore) {

                    /* keysOnly => no dups, so BINs only, not DBINs. */
                    if (childType != LogEntryType.LOG_BIN) {
                        return;
                    }
                    BIN bin = (BIN) node;
                    for (int i = 0; i < bin.getNEntries(); i += 1) {

                        if (bin.isEntryPendingDeleted(i) ||
                            bin.isEntryKnownDeleted(i)) {
                            continue;
                        }

                        Node child = bin.getTarget(i);
                        if (child == null || child.isLN()) {
                            byte[] lnKey = bin.getKey(i);
                            try {
                                KeyAndData e = new KeyAndData(lnKey);
                                while (!queue.offer(e, offerTimeout,
                                                    TimeUnit.MILLISECONDS)) {

                                    checkShutdown();
                                }
                            } catch (InterruptedException IE) {
                                setException(IE);
                            }
                        }
                    }
                }
            };
        } else {
            /* Keys and data are returned -- use default implementation. */
            return new DiskOrderedScanProcessor();
        }
    }

    private class DiskOrderedScanProcessor implements TreeNodeProcessor {

        /*
         * A place to stash any exception caught by the producer thread so that
         * it can be returned to the application.
         */
        private Throwable exception;

        private volatile boolean shutdownNow;

        public void processLSN(@SuppressWarnings("unused") long childLsn,
                               LogEntryType childType,
                               Node node,
                               byte[] lnKey) {

            checkShutdown();

            if (!childType.isLNType()) {
                return;
            }

            LN ln = (LN) node;
            try {
                KeyAndData e = new KeyAndData(lnKey, ln);
                while (!queue.offer(e, offerTimeout,
                                    TimeUnit.MILLISECONDS)) {
                    checkShutdown();
                }
            } catch (InterruptedException IE) {
                setException(IE);
                setShutdown();
            }
        }

        public void processDirtyDeletedLN(@SuppressWarnings("unused")
                                          long childLsn,
                                          @SuppressWarnings("unused")
                                          LN ln,
                                          @SuppressWarnings("unused")
                                          byte[] lnKey) {

            /* Use any opportunity to check shutdown status. */
            checkShutdown();
        }

        public void close() {
            try {
                if (!queue.offer(END_OF_QUEUE, offerTimeout,
                                 TimeUnit.MILLISECONDS)) {
                    /* Cursor.close() called, but queue was not drained. */
                    setException(SHUTDOWN_REQUESTED_EXCEPTION.
                                 fillInStackTrace());
                    setShutdown();
                }
            } catch (InterruptedException IE) {
                setException(IE);
                setShutdown();
            }
        }

        protected void setException(Throwable t) {
            exception = t;
        }

        private Throwable getException() {
            return exception;
        }

        private void setShutdown() {
            shutdownNow = true;
        }

        protected void checkShutdown() {
            if (shutdownNow) {
                throw SHUTDOWN_REQUESTED_EXCEPTION;
            }
        }

        public void noteMemoryExceeded() {
        }
    }

    /**
     * Process notifications of mutable property changes.
     *
     * @throws IllegalArgumentException via Environment ctor and
     * setMutableConfig.
     */
    public void envConfigUpdate(final DbConfigManager cm,
                                @SuppressWarnings("unused")
                                final EnvironmentMutableConfig ignore)
        throws DatabaseException {

        offerTimeout =
            cm.getDuration(EnvironmentParams.DOS_PRODUCER_QUEUE_TIMEOUT);
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    public synchronized void close() {
        if (closed) {
            return;
        }

        /* Tell Producer Thread to die if it hasn't already. */
        callback.setShutdown();

        closed = true;
    }

    public void checkEnv() {
        dbImpl.getDbEnvironment().checkIfInvalid();
    }

    private OperationStatus setData(final DatabaseEntry foundKey,
                                    final DatabaseEntry foundData) {

        if (keysOnly) {
            if (foundKey == null) {
                return OperationStatus.KEYEMPTY;
            }
            final byte[] keyBytes = currentNode.getKey();
            if (dups) {
                DupKeyData.split(keyBytes, keyBytes.length, foundKey, null);
            } else {
                LN.setEntry(foundKey, keyBytes);
            }
        } else {
            final byte[] keyBytes = currentNode.getKey();
            if (dups) {
                DupKeyData.split(keyBytes, keyBytes.length, foundKey,
                                 foundData);
            } else {

                /*
                 * For dirty-read, the data of the LN can be set to null at any
                 * time.
                 */
                final byte[] lnData =
                    (currentNode == null) ? null : currentNode.getData();
                if (lnData == null) {
                    return OperationStatus.KEYEMPTY;
                }

                LN.setEntry(foundData, lnData);
                LN.setEntry(foundKey, keyBytes);
            }
        }
        return OperationStatus.SUCCESS;
    }

    public synchronized
        OperationStatus getCurrent(final DatabaseEntry foundKey,
                                   final DatabaseEntry foundData) {

        if (closed) {
            throw new IllegalStateException("ForwardCursor not initialized");
        }

        if (currentNode == END_OF_QUEUE) {
            return OperationStatus.KEYEMPTY;
        }

        return setData(foundKey, foundData);
    }

    public synchronized
        OperationStatus getNext(final DatabaseEntry foundKey,
                                final DatabaseEntry foundData) {

        if (closed) {
            throw new IllegalStateException("ForwardCursor not initialized");
        }

        /*
         * If NOTFOUND was returned earlier, do not enter loop below to avoid a
         * hang.  [#21282]
         */
        if (currentNode == END_OF_QUEUE) {
            return OperationStatus.NOTFOUND;
        }

        try {

            /*
             * Poll in a loop in case the producer thread throws an exception
             * and can't put END_OF_QUEUE on the queue because of an
             * InterruptedException.  The presence of an exception is the last
             * resort to make sure that getNext actually returns to the user.
             */
            do {
                currentNode = queue.poll(1, TimeUnit.SECONDS);
                if (callback.getException() != null) {
                    break;
                }
            } while (currentNode == null);
        } catch (InterruptedException IE) {
            currentNode = END_OF_QUEUE;
        }

        if (callback.getException() != null) {
            throw new DiskOrderedCursorProducerException
                ("Producer Thread Failure", callback.getException());
        }

        if (currentNode == END_OF_QUEUE) {
            return OperationStatus.NOTFOUND;
        }

        return setData(foundKey, foundData);
    }
}
