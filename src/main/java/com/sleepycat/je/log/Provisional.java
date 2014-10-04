/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.log;

import com.sleepycat.je.utilint.DbLsn;

/**
 * Specifies whether to log an entry provisionally.
 *
 * Provisional log entries:
 * 
 * What are provisional log entries?
 *
 *    Provisional log entries are those tagged with the provisional attribute
 *    in the log entry header. The provisional attribute can be applied to any
 *    type of log entry, and is implemented in
 *    com.sleepycat.je.log.LogEntryHeader as two stolen bits in the 8 bit
 *    version field.
 *
 * When is the provisional attribute used?
 * 
 *    The provisional attribute is used only during recovery. It very simply
 *    indicates that recovery will ignore and skip over this log entry.
 * 
 * When is the provisional attribute set?
 * 
 *    The provisional attribute started out as a way to create atomicity among
 *    different log entries. Child pointers in the JE Btree are physical LSNs,
 *    so each Btree node's children must be logged before it in the log. On
 *    the other hand, one fundamental assumption of the JE log is that each
 *    Btree node found in the log can be replayed and placed in the in-memory
 *    tree. To do so, each Btree node must have a parent node that will house
 *    it. The grouping of multiple log entries into one atomic group is often
 *    used to fulfiil this requirement.
 * 
 *     * Atomic log entries:
 *
 *           + When a btree is first created, we bootstrap tree creation by
 *           logging the first BIN provisionally, then creating a parent IN
 *           which is the Btree root IN, which points to this first BIN.
 *
 *           + When we split a Btree node, we create a new IN, which is the
 *           sibling of the split node. We log the old sibling and the new
 *           sibling provisionally, and then log the parent, so that any
 *           crashes in the middle of this triumvirate which result in the
 *           failure to log the parent will skip over the orphaned siblings.
 *
 *           + Splitting the Btree root is just a special case of a split.
 *
 *           + Creating a duplicate subtree to hang in the middle of a btree is
 *           just a special case of a split and btree first creation.
 *
 *     * Entries not meant to be recovered
 *
 *           Temp DBs are not meant to be recovered and we log their LN
 *           and IN nodes in a very lax fashion, purely as a way of evicting
 *           them out of the cache temporarily. There is no guarantee that a
 *           consistent set has been logged to disk. We skip over them for both
 *           recovery performance and the "each-node-must-have-a-parent" rule.
 *
 *     * Durable deferred-write entries
 *
 *           Deferred-write INs are logged provisionally for the same reasons
 *           as for temp DBs (above): for recovery performance and the
 *           "each-node-must-have-a-parent" rule.
 *
 *           Deferred-write LNs are logged non-provisionally to support
 *           obsolete LSN counting.  It would be nice to log them provisionally
 *           for recovery performance and to allow LN deletion without logging;
 *           however, that is not currently practical if obsolete counting is
 *           to be supported.  See [#16864].
 *
 *     * Checkpoint performance
 *
 *           When we flush a series of nodes, it's a waste to replay nodes
 *           which are referenced by higher levels. For example, if we
 *           checkpoint this btree:
 * 
 *           INA -> INb -> BINc (dirty)-> LNd
 * 
 *           we log them in this order:
 * 
 *           BINc
 *           INb
 * 
 *           And there's no point to replaying BINc, because it's referenced by
 *           INb.  We skip over BINc, which we do by logging it provisionally.
 *
 *           In addition, BEFORE_CKPT_END is used to improve cleaner
 *           performance by keeping utilization information up-to-date during
 *           the checkpoint.  See below for details.
 * 
 *     * Log cleaning - removing references to deleted files.
 * 
 *       When we delete a file for log cleaning we guarantee that no active log
 *       entries refer to any log entry in the deleted file. Suppose our
 *       checkpoint looks like this:
 * 
 *         5/100 LNa
 *         5/200 Ckpt start
 *         5/300 INs
 *         ...
 *         5/500 Ckpt end
 *         ...
 *         5/800 last entry in log
 * 
 *       Because we do not delete a file until the Ckpt end after processing
 *       (cleaning) it, nothing from 5/500 to 5/800 can refer to a file deleted
 *       due to the Ckpt end in 5/500.
 *
 *       BEFORE_CKPT_END is motivated in part (see below for a complete
 *       description) by the fact that while log entries between 5/100
 *       (first active lsn) and 5/500 (ckpt end) will not in of themselves
 *       contain a LSN for a cleaned, deleted file, the act of processing them
 *       during recovery could require fetching a node from a deleted file. For
 *       example, the IN at 5/300 could have an in-memory parent which has a
 *       reference to an older, cleaned version of that IN.  Treating the span
 *       between 5/200 and 5/500 as provisional is both optimal, because only
 *       the high level INs need to be processed, and required, in order not to
 *       fetch from a cleaned file. See [#16037].
 *
 * Provisional.BEFORE_CKPT_END
 * ---------------------------
 * This property was added to solve a specific problem that occurs in earlier
 * versions of JE:  When a long checkpoint runs, the BINs are not counted
 * obsolete until after the entire BIN level has been logged.  Specifically,
 * they are counted obsolete when their ancestor is logged non-provisionally.
 * Most INs logged by a checkpoint are BINs.  This means that during a very
 * long checkpoint, cleaning of the files containing those old BINs is delayed,
 * and more importantly the calculated utilization is much higher than it
 * actually is.  The correction in utilization does not occur until the end of
 * the checkpoint, when the higher level INs are logged.  This manifests as a
 * lull in cleaning during the checkpoint, because calculated utilization is
 * artificially high, and a spike in cleaning at the end of the checkpoint.  In
 * some cases, the cleaner cannot recover from the backlog that is created by
 * the spike.
 *
 * The provisional property effects obsolete counting as follows:
 *
 *  + If an IN is logged with Provisional.YES, the old version of the IN is not
 *    counted obsolete immediately.  Instead, the offset of the old version of
 *    the IN is added to a list in its parent IN.  The offsets migrate upward
 *    in the tree in this manner until an ancestor IN is logged
 *    non-provisionally.
 *
 *  + If an IN is logged with Provisional.NO or BEFORE_CKPT_END, the old
 *    version of the IN is counted obsolete immediately (and offsets
 *    accumulated from provisional child INs are counted).  This means
 *    that the obsolete offset is added to the UtilizationTracker, and may be
 *    flushed in a FileSummaryLN any time after that.  At the latest, it is
 *    flushed at the end of the checkpoint.
 *
 * Because subtree logging is now used for checkpoints and the parent IN of
 * each logged sub-tree is logged with BEFORE_CKPT_END, the prior version of
 * all INs in the sub-tree will be counted obsolete at that time.  This keeps
 * the calculated utilization accurate throughout the checkpoint, and prevents
 * the large per-checkpoint lull and spike in log cleaning.
 *
 * For the intermediate levels, Provisional.BEFORE_CKPT_END must be used rather
 * than Provisional.NO, which is reserved for the highest level only.  During
 * recovery, the Provisional values are treated as follows (this is from the
 * Provisional javadoc):
 *  + NO:  The entry is non-provisional and is always processed by recovery.
 *  + YES: The entry is provisional and is never processed by recovery.
 *  + BEFORE_CKPT_END: The entry is provisional (not processed by recovery) if
 *    it occurs before the CkptEnd in the recovery interval, or is
 *    non-provisional (is processed) if it occurs after CkptEnd.
 *
 * The key to BEFORE_CKPT_END is that it is treated as provisional if a CkptEnd
 * is logged, i.e., if we do not crash before completing the checkpoint.
 * Because the checkpoint completed, we may have deleted log files that
 * would be necessary to replay the IN.  So we cannot safely replay it.
 *
 * Note the difference in the treatment of BEFORE_CKPT_END for obsolete
 * counting and recovery:
 *  + For obsolete counting, BEFORE_CKPT_END is treated as non-provisional.
 *  + For recovery, when the IN occurs before CkptEnd, BEFORE_CKPT_END is
 *    treated as provisional.
 * This difference is the reason for the existence of BEFORE_CKPT_END.
 */
public enum Provisional {

    /**
     * The entry is non-provisional and is always processed by recovery.
     */
    NO,
    
    /**
     * The entry is provisional and is never processed by recovery.
     */
    YES,
    
    /**
     * The entry is provisional (not processed by recovery) if it occurs before
     * the CkptEnd in the recovery interval, or is non-provisional (is
     * processed) if it occurs after CkptEnd.
     */
    BEFORE_CKPT_END;

    /**
     * Determines whether a given log entry should be processed during
     * recovery.
     */
    public boolean isProvisional(long logEntryLsn, long ckptEndLsn) {
        assert logEntryLsn != DbLsn.NULL_LSN;
        switch (this) {
        case NO:
            return false;
        case YES:
            return true;
        case BEFORE_CKPT_END:
            return ckptEndLsn != DbLsn.NULL_LSN &&
                   DbLsn.compareTo(logEntryLsn, ckptEndLsn) < 0;
        default:
            assert false;
            return false;
        }
    }
}
