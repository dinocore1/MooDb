/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.stream;

import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.ACK_WAIT_MS;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TOTAL_TXN_MS;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TXNS_ACKED;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TXNS_NOT_ACKED;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.AtomicLongStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.VLSN;

/**
 * FeederTxns manages transactions that need acknowledgments.
 */
public class FeederTxns {

    /*
     * Tracks transactions that have not yet been acknowledged for the entire
     * replication node.
     */
    private final Map<Long, TxnInfo> txnMap;

    private final RepImpl repImpl;
    private final StatGroup statistics;
    private final AtomicLongStat txnsAcked;
    private final AtomicLongStat txnsNotAcked;
    private final AtomicLongStat ackWaitMs;
    private final AtomicLongStat totalTxnMs;

    public FeederTxns(RepImpl repImpl) {

        txnMap = new ConcurrentHashMap<Long, TxnInfo>();
        this.repImpl = repImpl;
        statistics = new StatGroup(FeederTxnStatDefinition.GROUP_NAME,
                                   FeederTxnStatDefinition.GROUP_DESC);
        txnsAcked = new AtomicLongStat(statistics, TXNS_ACKED);
        txnsNotAcked = new AtomicLongStat(statistics, TXNS_NOT_ACKED);
        ackWaitMs = new AtomicLongStat(statistics, ACK_WAIT_MS);
        totalTxnMs = new AtomicLongStat(statistics, TOTAL_TXN_MS);
    }

    /**
     * Create a new TxnInfo so that transaction commit can wait onthe latch it
     * sets up.
     *
     * @param txn identifies the transaction.
     */
    public void setupForAcks(MasterTxn txn) {
        if (txn.getRequiredAckCount() == 0) {
            /* No acks called for, no setup needed. */
            return;
        }
        TxnInfo txnInfo = new TxnInfo(txn);
        TxnInfo  prevInfo = txnMap.put(txn.getId(), txnInfo);
        assert(prevInfo == null);
    }

    /**
     * Returns the transaction if it's waiting for acknowledgments. Returns
     * null otherwise.
     */
    public MasterTxn getAckTxn(long txnId) {
        TxnInfo txnInfo = txnMap.get(txnId);
        return (txnInfo == null) ? null : txnInfo.txn;
    }

    /*
     * Clears any ack requirements associated with the transaction. It's
     * typically invoked on a transaction abort.
     */
    public void clearTransactionAcks(Txn txn) {
        txnMap.remove(txn.getId());
    }

    /**
     * Notes that an acknowledgment was received from a replica.
     *
     * @param txnId the locally committed transaction that was acknowledged.
     *
     * @return the transaction VLSN, if txnId needs an ack, null otherwise
     */
    public VLSN noteReplicaAck(long txnId) {
        final TxnInfo txnInfo = txnMap.get(txnId);
        if (txnInfo == null) {
            return null;
        }
        txnInfo.countDown();
        return txnInfo.getCommitVLSN();
    }

    /**
     * Waits for the required number of replica acks to come through.
     *
     * @param txn identifies the transaction to wait for.
     *
     * @param timeoutMs the amount of time to wait for the acknowledgments
     * before giving up.
     *
     * @throws InsufficientAcksException if the ack requirements were not met
     */
    public void awaitReplicaAcks(MasterTxn txn, int timeoutMs)
        throws InterruptedException {

        TxnInfo txnInfo = txnMap.get(txn.getId());
        if (txnInfo == null) {
            return;
        }
        txnInfo.await(timeoutMs);
        txnMap.remove(txn.getId());
        int pendingAcks = txnInfo.getPendingAcks();

        if (pendingAcks > 0) {
            final int requiredAcks = txn.getCurrentRequiredAckCount();
            int requiredAckDelta = txn.getRequiredAckCount() -
                requiredAcks;
            if (requiredAckDelta >= pendingAcks) {

                /*
                 * The group size was reduced while waiting for acks and the
                 * acks received are sufficient given the new reduced group
                 * size.
                 */
                return;
            }
            /* Snapshot the state to be used in the error message */
            final String dumpState = repImpl.dumpFeederState();
            final FeederManager feederManager =
                repImpl.getRepNode().feederManager();
            int currentFeederCount =
                feederManager.getNumCurrentAckFeeders(txn.getCommitVLSN());

            /*
             * Repeat the check to ensure that acks have not been received in
             * the time between the completion of the await() call above and
             * the creation of the exception message. This tends to happen when
             * there are lots of threads in the process thus potentially
             * delaying the resumption of this thread following the timeout
             * resulting from the await.
             */
            if (currentFeederCount >= requiredAcks) {
                String msg = "txn " + txn.getId() +
                " commit vlsn:" + txnInfo.getCommitVLSN() +
                " acknowledged after explicit feeder check" +
                " latch count:" + txnInfo.getPendingAcks() +
                " state:" + dumpState +
                " required acks:" + requiredAcks;

                LoggerUtils.info(repImpl.getLogger(), repImpl, msg);
                return;
            }

            /*
             * We can avoid the exception if we're a Designated Primary.  It's
             * useful to check for this again here in case we happen to lose
             * the connection to the replica in the (brief) period since the
             * pre-log hook.  Note that in this case we merely want to check;
             * we don't want to switch into "active primary" mode unless/until
             * we actually lose the connection to the replica.
             */
            if (repImpl.getRepNode().checkDesignatedPrimary()) {
                return;
            }
            throw new InsufficientAcksException
                (txn, pendingAcks, timeoutMs, dumpState);
        }
    }

    /*
     * @see com.sleepycat.je.rep.stream.FeederSource#close()
     */
    public void close() {
        /* Free any blocked commits. */

        for (TxnInfo txnInfo : txnMap.values()) {
            long remaining = txnInfo.getPendingAcks();
            for (long i = 0; i < remaining; i++) {
                txnInfo.countDown();
            }
        }
    }

    /**
     * Used to track the latch and the transaction information associated with
     * a transaction needing an acknowledgment.
     */
    private class TxnInfo {
        /* The latch used to track transaction acknowledgments. */
        final private CountDownLatch latch;
        final MasterTxn txn;

        private TxnInfo(MasterTxn txn) {
            assert(txn != null);
            final int numRequiredAcks = txn.getRequiredAckCount();
            this.latch = (numRequiredAcks == 0) ?
                null :
                new CountDownLatch(numRequiredAcks);
            this.txn = txn;
        }

        /**
         * Returns the VLSN associated with the committed txn, or null if the
         * txn has not yet been committed.
         */
        public VLSN getCommitVLSN() {
            return txn.getCommitVLSN();
        }

        private final boolean await(int timeoutMs)
            throws InterruptedException {

            final long ackAwaitStartMs = System.currentTimeMillis();
            boolean isZero = (latch == null) ||
                latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            if (isZero) {
                txnsAcked.increment();
                final long now = System.currentTimeMillis();
                ackWaitMs.add(now - ackAwaitStartMs);
                totalTxnMs.add(now - txn.getStartMs());
            } else {
                txnsNotAcked.increment();
            }
            return isZero;
        }

        private final void countDown() {
            if (latch == null) {
                return;
            }

            latch.countDown();
        }

        private final int getPendingAcks() {
            if (latch == null) {
                return 0;
            }

            return (int) latch.getCount();
        }
    }

    public StatGroup getStats() {
        StatGroup ret = statistics.cloneGroup(false);

        return ret;
    }

    public void resetStats() {
        statistics.clear();
    }

    public StatGroup getStats(StatsConfig config) {

        StatGroup cloneStats = statistics.cloneGroup(config.getClear());

        return cloneStats;
    }
}
