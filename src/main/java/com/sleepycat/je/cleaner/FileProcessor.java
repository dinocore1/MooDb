/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.cleaner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.log.CleanerFileReader;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINDelta;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Reads all entries in a log file and either determines them to be obsolete or
 * marks them for migration.  LNs are marked for migration by setting the BIN
 * entry MIGRATE flag.  INs are marked for migration by setting the dirty flag.
 *
 * May be invoked explicitly by calling doClean, or woken up if used as a
 * daemon thread.
 */
class FileProcessor extends DaemonThread {

    /**
     * The number of LN log entries after we process pending LNs.  If we do
     * this too seldom, the pending LN queue may grow large, and it isn't
     * budgeted memory.  If we process it too often, we will repeatedly request
     * a non-blocking lock for the same locked node.
     */
    private static final int PROCESS_PENDING_EVERY_N_LNS = 100;

    /**
     * Whether to prohibit BINDeltas for a BIN that is fetched by the cleaner.
     * The theory is that when fetching a BIN during cleaning we normally
     * expect that the BIN will be evicted soon, and a delta during checkpoint
     * would be wasted.  However, this does not take into account use of the
     * BIN by the application after fetching; the BIN could become hot and then
     * deltas may be profitable.  To be safe we currently allow deltas when
     * fetching.
     */
    private static final boolean PROHIBIT_DELTAS_WHEN_FETCHING = false;

    private EnvironmentImpl env;
    private Cleaner cleaner;
    private FileSelector fileSelector;
    private UtilizationProfile profile;
    private UtilizationCalculator calculator;

    /* Log version for the target file. */
    private int fileLogVersion;

    /* Per Run counters. Reset before each file is processed. */
    private int nINsObsoleteThisRun = 0;
    private int nINsCleanedThisRun = 0;
    private int nINsDeadThisRun = 0;
    private int nINsMigratedThisRun = 0;
    private int nBINDeltasObsoleteThisRun = 0;
    private int nBINDeltasCleanedThisRun = 0;
    private int nBINDeltasDeadThisRun = 0;
    private int nBINDeltasMigratedThisRun = 0;
    private int nLNsObsoleteThisRun = 0;
    private int nLNsCleanedThisRun = 0;
    private int nLNsDeadThisRun = 0;
    private int nLNsLockedThisRun = 0;
    private int nLNsMigratedThisRun = 0;
    private int nLNsMarkedThisRun = 0;
    private int nLNQueueHitsThisRun = 0;
    private int nEntriesReadThisRun;
    private long nRepeatIteratorReadsThisRun;

    FileProcessor(String name,
                  EnvironmentImpl env,
                  Cleaner cleaner,
                  UtilizationProfile profile,
                  UtilizationCalculator calculator,
                  FileSelector fileSelector) {
        super(0, name, env);
        this.env = env;
        this.cleaner = cleaner;
        this.fileSelector = fileSelector;
        this.profile = profile;
        this.calculator = calculator;
    }

    public void clearEnv() {
        env = null;
        cleaner = null;
        fileSelector = null;
        profile = null;
        calculator = null;
    }

    /**
     * Return the number of retries when a deadlock exception occurs.
     */
    @Override
    protected long nDeadlockRetries() {
        return cleaner.nDeadlockRetries;
    }

    /**
     * Activates the cleaner.  Is normally called when je.cleaner.byteInterval
     * bytes are written to the log.
     */
    @Override
    public void onWakeup()
        throws DatabaseException {

        doClean(true,   // invokedFromDaemon
                true,   // cleanMultipleFiles
                false); // forceCleaning
    }

    /**
     * Cleans selected files and returns the number of files cleaned.  May be
     * called by the daemon thread or programatically.
     *
     * @param invokedFromDaemon currently has no effect.
     *
     * @param cleanMultipleFiles is true to clean until we're under budget,
     * or false to clean at most one file.
     *
     * @param forceCleaning is true to clean even if we're not under the
     * utilization threshold.
     *
     * @return the number of files cleaned, not including files cleaned
     * unsuccessfully.
     */
    public synchronized int doClean(boolean invokedFromDaemon,
                                    boolean cleanMultipleFiles,
                                    boolean forceCleaning)
        throws DatabaseException {

        if (env.isClosed()) {
            return 0;
        }

        assert env.getSyncCleanerBarrier() != null;

        /*
         * Get all file summaries including tracked files.  Tracked files may
         * be ready for cleaning if there is a large cache and many files have
         * not yet been flushed and do not yet appear in the profile map.
         */
        SortedMap<Long, FileSummary> fileSummaryMap =
            profile.getFileSummaryMap(true /*includeTrackedFiles*/);

        /* Clean until no more files are selected.  */
        final int nOriginalLogFiles = fileSummaryMap.size();
        int nFilesCleaned = 0;
        while (true) {

            /* Don't clean forever. */
            if (nFilesCleaned >= nOriginalLogFiles) {
                LoggerUtils.logMsg(logger, env, Level.INFO,
                                   "Maximum files cleaned for one run. " +
                                   fileSelector);
                break;
            }

            /* Stop if the daemon is paused or the environment is closing. */
            if ((invokedFromDaemon && isPaused()) || env.isClosing()) {
                break;
            }

            /*
             * Process pending LNs periodically.  Pending LNs can prevent file
             * deletion.  Do not call deleteSafeToDeleteFiles here, since
             * cleaner threads will block while the checkpointer deletes log
             * files, which can be time consuming.
             */
            cleaner.processPending();

            /*
             * To avoid a deadlock, set protected files in calculator before
             * synchronizing on FileSelector (in selectFileForCleaning below).
             */
            calculator.setProtectedFiles();

            /*
             * Select the next file for cleaning and update the Cleaner's
             * read-only file collections.
             */
            final boolean needLowUtilizationSet =
                cleaner.clusterResident || cleaner.clusterAll;

            Long fileNum = fileSelector.selectFileForCleaning
                (calculator, fileSummaryMap, forceCleaning,
                 needLowUtilizationSet, cleaner.maxBatchFiles);

            cleaner.updateReadOnlyFileCollections();

            /*
             * If no file was selected above, the estimated total utilization
             * is above the minUtilization threshold.  However, we may need to
             * read the log file to correct utilization.
             */
            boolean calcUtilizationOnly = false;
            if (fileNum == null) {
                fileNum = fileSelector.selectFileForCorrection(calculator,
                                                               fileSummaryMap);
                calcUtilizationOnly = true;
            }

            /*
             * Stop if no file is selected for cleaning or utilization
             * correction.
             */
            if (fileNum == null) {
                break;
            }

            /* Keep track of estimated and true utilization. */
            final FileSummary estimatedFileSummary;
            if (fileSummaryMap.containsKey(fileNum)) {
                estimatedFileSummary = fileSummaryMap.get(fileNum).clone();
            } else {
                estimatedFileSummary = null;
            }
            final FileSummary recalculatedFileSummary = new FileSummary();
            final INSummary inSummary = new INSummary();
            final UtilizationCalculator calculator =
                cleaner.getUtilizationCalculator();
            final float lnSizeCorrection =
                calculator.getLNSizeCorrectionFactor();

            /*
             * Process the selected file.
             */
            resetPerRunCounters();
            boolean finished = false;
            boolean fileDeleted = false;
            final long fileNumValue = fileNum.longValue();
            cleaner.nCleanerRuns.increment();
            if (calcUtilizationOnly) {
                cleaner.nCleanerProbeRuns.increment();
            }
            final long runId = cleaner.totalRuns.incrementAndGet();
            final MemoryBudget budget = env.getMemoryBudget();
            try {
                TestHookExecute.doHookIfSet(cleaner.fileChosenHook);

                /* Trace is unconditional for log-based debugging. */
                final String traceMsg =
                    "CleanerRun " + runId +
                    " on file 0x" + Long.toHexString(fileNumValue) +
                    " begins" +
                    " probe=" + calcUtilizationOnly +
                    " backlog=" + fileSelector.getBacklog();
                Trace.trace(envImpl, traceMsg);

                /* Also log FINE level message. */
                LoggerUtils.logMsg(logger, env, Level.FINE, traceMsg);

                /* Process all log entries in the file. */
                if (processFile(fileNum, recalculatedFileSummary,
                                inSummary, calcUtilizationOnly)) {
                    /* File is fully processed, update stats. */
                    nFilesCleaned += 1;
                    accumulatePerRunCounters();
                    finished = true;
                    if (estimatedFileSummary != null) {
                        calculator.adjustUtilization(fileNum,
                                                     fileSummaryMap.lastKey(),
                                                     estimatedFileSummary,
                                                     recalculatedFileSummary);
                    }
                }
            } catch (FileNotFoundException e) {

                /*
                 * File was deleted.  Although it is possible that the file was
                 * deleted externally it is much more likely that the file was
                 * deleted normally after being cleaned earlier.  This can
                 * occur when tracked obsolete information is collected and
                 * processed after the file has been cleaned and deleted.
                 * Since the file does not exist, ignore the error so that the
                 * cleaner will continue.  The tracing below will indicate that
                 * the file was deleted.  Remove the file completely from the
                 * FileSelector and UtilizationProfile so that we don't
                 * repeatedly attempt to process it. [#15528]
                 */
                fileDeleted = true;
                profile.removeFile(fileNum, null /*databases*/);
                fileSelector.removeAllFileReferences(fileNum, budget);
            } catch (IOException e) {
                LoggerUtils.traceAndLogException(env, "Cleaner", "doClean", "",
                                                 e);
                throw new EnvironmentFailureException
                    (env, EnvironmentFailureReason.LOG_INTEGRITY, e);
            } catch (RuntimeException e) {
                /* Log all exceptions during cleaning. [#21328] */
                LoggerUtils.traceAndLogException(env, "Cleaner", "doClean", "",
                                                 e);
                throw e;
            } finally {
                if (!finished && !fileDeleted && !calcUtilizationOnly) {
                    fileSelector.putBackFileForCleaning(fileNum);
                }
                
                /* Trace is unconditional for log-based debugging. */
                final String traceMsg =
                    "CleanerRun " + runId +
                    " ends on file 0x" + Long.toHexString(fileNumValue) +
                    " probe=" + calcUtilizationOnly +
                    " invokedFromDaemon=" + invokedFromDaemon +
                    " finished=" + finished +
                    " fileDeleted=" + fileDeleted +
                    " nEntriesRead=" + nEntriesReadThisRun +
                    " nINsObsolete=" + nINsObsoleteThisRun +
                    " nINsCleaned=" + nINsCleanedThisRun +
                    " nINsDead=" + nINsDeadThisRun +
                    " nINsMigrated=" + nINsMigratedThisRun +
                    " nBINDeltasObsolete=" + nBINDeltasObsoleteThisRun +
                    " nBINDeltasCleaned=" + nBINDeltasCleanedThisRun +
                    " nBINDeltasDead=" + nBINDeltasDeadThisRun +
                    " nBINDeltasMigrated=" + nBINDeltasMigratedThisRun +
                    " nLNsObsolete=" + nLNsObsoleteThisRun +
                    " nLNsCleaned=" + nLNsCleanedThisRun +
                    " nLNsDead=" + nLNsDeadThisRun +
                    " nLNsMigrated=" + nLNsMigratedThisRun +
                    " nLNsMarked=" + nLNsMarkedThisRun +
                    " nLNQueueHits=" + nLNQueueHitsThisRun +
                    " nLNsLocked=" + nLNsLockedThisRun;
                Trace.trace(envImpl, traceMsg);

                /* Only construct INFO level message if needed. */
                if (logger.isLoggable(Level.INFO)) {
                    final int estUtil;
                    final int corUtil;
                    if (estimatedFileSummary != null) {
                        estUtil =
                            estimatedFileSummary.utilization();
                        corUtil =
                            estimatedFileSummary.utilization(lnSizeCorrection);
                    } else {
                        estUtil = -1;
                        corUtil = -1;
                    }
                    final int recalcUtil =
                        recalculatedFileSummary.utilization();
                    final String logMsg = traceMsg +
                        " logSummary=" + cleaner.getLogSummary() +
                        " inSummary=" + inSummary +
                        " estFileSummary=" + estimatedFileSummary +
                        " recalcFileSummary=" + recalculatedFileSummary +
                        " lnSizeCorrection=" + lnSizeCorrection +
                        " newLnSizeCorrection=" +
                        calculator.getLNSizeCorrectionFactor() +
                        " estimatedUtilization=" + estUtil +
                        " correctedUtilization=" + corUtil +
                        " recalcUtilization=" + recalcUtil;
                    LoggerUtils.logMsg(logger, env, Level.INFO, logMsg);
                }
            }

            /* If we should only clean one file, stop now. */
            if (!cleanMultipleFiles) {
                break;
            }

            /* Refresh file summary info for next file selection. */
            fileSummaryMap =
                profile.getFileSummaryMap(true /*includeTrackedFiles*/);
        }

        return nFilesCleaned;
    }

    /**
     * Process all log entries in the given file.
     *
     * Note that we gather obsolete offsets at the beginning of the method and
     * do not check for obsolete offsets of entries that become obsolete while
     * the file is being processed.  An entry in this file can become obsolete
     * before we process it because: 1- normal application activity deletes or
     * updates the entry, 2- proactive migration migrates the entry.  However,
     * large numbers of entries also become obsolete as the result of LN
     * migration while processing the file, when lazyMigration is false.
     * Checking the TrackedFileSummary while processing the file would be
     * expensive if it has many entries, because we perform a linear search in
     * the TFS.  There is a tradeoff between the cost of the TFS lookup and its
     * benefit, which is to avoid a tree search if the entry is obsolete.  Many
     * more lookups for non-obsolete entries than obsolete entries will
     * typically be done.  Because of the high cost of the linear search,
     * especially when processing large log files, we do not check the TFS.
     * [#19626]
     *
     * @param fileNum the file being cleaned.
     *
     * @param fileSummary used to return the true utilization.
     *
     * @param calcUtilizationOnly is true if we should calculate the true
     * utilization of the file but not process the entries.
     *
     * @return false if we aborted file processing because the environment is
     * being closed.
     */
    private boolean processFile(Long fileNum,
                                FileSummary fileSummary,
                                INSummary inSummary,
                                boolean calcUtilizationOnly)
        throws DatabaseException, IOException {

        /* Get the current obsolete offsets for this file. */
        final PackedOffsets obsoleteOffsets =
            profile.getObsoleteDetail(fileNum, true /*logUpdate*/);
        final PackedOffsets.Iterator obsoleteIter = obsoleteOffsets.iterator();
        long nextObsolete = -1;

        /* Copy to local variables because they are mutable properties. */
        final int readBufferSize = cleaner.readBufferSize;
        final int lookAheadCacheSize =
            calcUtilizationOnly ? 0 : cleaner.lookAheadCacheSize;

        /*
         * Add the overhead of this method to the budget.  Two read buffers are
         * allocated by the file reader. The log size of the offsets happens to
         * be the same as the memory overhead.
         */
        final int adjustMem = (2 * readBufferSize) +
                               obsoleteOffsets.getLogSize() +
                               lookAheadCacheSize;
        final MemoryBudget budget = env.getMemoryBudget();
        budget.updateAdminMemoryUsage(adjustMem);

        /* Evict after updating the budget. */
        if (Cleaner.DO_CRITICAL_EVICTION) {
            env.daemonEviction(true /*backgroundIO*/);
        }

        /*
         * We keep a look ahead cache of non-obsolete LNs.  When we lookup a
         * BIN in processLN, we also process any other LNs in that BIN that are
         * in the cache.  This can reduce the number of tree lookups.
         */
        final LookAheadCache lookAheadCache = calcUtilizationOnly ?
            null : new LookAheadCache(lookAheadCacheSize);

        /*
         * For obsolete entries we must check for pending deleted DBs.  To
         * avoid the overhead of DbTree.getDb on every entry we keep a set of
         * all DB IDs encountered and do the check once per DB at the end.
         */
        final Set<DatabaseId> checkPendingDbSet = calcUtilizationOnly ?
            null : new HashSet<DatabaseId>();

        /*
         * Use local caching to reduce DbTree.getDb overhead.  Do not call
         * releaseDb after getDb with the dbCache, since the entire dbCache
         * will be released at the end of this method.
         */
        final Map<DatabaseId, DatabaseImpl> dbCache =
            new HashMap<DatabaseId, DatabaseImpl>();
        final DbTree dbMapTree = env.getDbTree();

        /* Keep track of all database IDs encountered. */
        final Set<DatabaseId> databases = new HashSet<DatabaseId>();

        /* Create the file reader. */
        final CleanerFileReader reader = new CleanerFileReader
            (env, readBufferSize, DbLsn.makeLsn(fileNum, 0), fileNum,
             fileSummary, inSummary);
        /* Validate all entries before ever deleting a file. */
        reader.setAlwaysValidateChecksum(true);
        try {
            final TreeLocation location = new TreeLocation();

            int nProcessedLNs = 0;
            int nProcessedEntries = 0;
            while (reader.readNextEntryAllowExceptions()) {
                cleaner.nEntriesRead.increment();
                nProcessedEntries += 1;
                long logLsn = reader.getLastLsn();
                long fileOffset = DbLsn.getFileOffset(logLsn);
                boolean isLN = reader.isLN();
                boolean isIN = reader.isIN();
                boolean isBINDelta = reader.isBINDelta();
                boolean isDbTree = reader.isDbTree();
                boolean isObsolete = false;

                /* Maintain a set of all databases encountered. */
                final DatabaseId dbId = reader.getDatabaseId();
                DatabaseImpl db = null;
                if (dbId != null) {
                    databases.add(dbId);

                    /*
                     * Clear DB cache after dbCacheClearCount entries, to
                     * prevent starving other threads that need exclusive
                     * access to the MapLN (for example, DbTree.deleteMapLN).
                     * [#21015]
                     */
                    if ((nProcessedEntries % cleaner.dbCacheClearCount) == 0) {
                        dbMapTree.releaseDbs(dbCache);
                        dbCache.clear();
                    }

                    db = dbMapTree.getDb(dbId, cleaner.lockTimeout, dbCache);

                    /*
                     * If the DB is gone, this entry is obsolete.  If delete
                     * cleanup is in progress, we will put the DB into the DB
                     * pending set further below.  This entry will be declared
                     * deleted after the delete cleanup is finished.
                     */
                    if (db == null || db.isDeleted()) {
                        isObsolete = true;
                    }
                }

                /* Remember the version of the log file. */
                if (reader.isFileHeader()) {
                    fileLogVersion = reader.getFileHeader().getLogVersion();
                }

                /* Stop if the daemon is shut down. */
                if (env.isClosing()) {
                    return false;
                }

                /* Update background reads. */
                int nReads = reader.getAndResetNReads();
                if (nReads > 0) {
                    env.updateBackgroundReads(nReads);
                }

                /* Sleep if background read/write limit was exceeded. */
                env.sleepAfterBackgroundIO();

                /* Check for a known obsolete node. */
                while (nextObsolete < fileOffset && obsoleteIter.hasNext()) {
                    nextObsolete = obsoleteIter.next();
                }
                if (nextObsolete == fileOffset) {
                    isObsolete = true;
                }

                /* Check for the entry type next because it is very cheap. */
                if (!isObsolete &&
                    !isLN &&
                    !isIN &&
                    !isBINDelta &&
                    !isDbTree) {
                    /* Consider all entries we do not process as obsolete. */
                    isObsolete = true;
                }

                /*
                 * Ignore deltas before log version 8.  Before the change to
                 * place deltas in the Btree (in JE 5.0), all deltas were
                 * considered obsolete by the cleaner.  Processing an old delta
                 * would be very wasteful (a Btree lookup, and possibly
                 * dirtying and flushing a BIN), and for duplicates databases
                 * could cause an exception due to the key format change.
                 * [#21405]
                 */
                if (!isObsolete &&
                    isBINDelta &&
                    fileLogVersion < 8) {
                    isObsolete = true;
                }

                /*
                 * Also ignore INs in dup DBs before log version 8.  These must
                 * be obsolete, just as DINs and DBINs must be obsolete (and
                 * are also ignored here) after dup DB conversion.  Also, the
                 * old format IN key cannot be used for lookups. [#21405]
                 */
                if (!isObsolete &&
                    isIN &&
                    db.getSortedDuplicates() &&
                    fileLogVersion < 8) {
                    isObsolete = true;
                }

                /*
                 * SR 14583: In JE 2.0 and later we can assume that all
                 * deleted LNs are obsolete. Either the delete committed and
                 * the BIN parent is marked with a pending deleted bit, or the
                 * delete rolled back, in which case there is no reference
                 * to this entry. JE 1.7.1 and earlier require a tree lookup
                 * because deleted LNs may still be reachable through their BIN
                 * parents.
                 */
                if (!isObsolete &&
                    isLN &&
                    reader.isLNDeleted() &&
                    fileLogVersion > 2) {
                    /* Deleted LNs are always obsolete. */
                    isObsolete = true;
                }

                /* Skip known obsolete nodes. */
                if (isObsolete) {
                    /* Count obsolete stats. */
                    if (!calcUtilizationOnly) {
                        if (isLN) {
                            nLNsObsoleteThisRun++;
                        } else if (isBINDelta) {
                            nBINDeltasObsoleteThisRun++;
                        } else if (isIN) {
                            nINsObsoleteThisRun++;
                        }
                    }
                    /* Update the pending DB set for obsolete entries. */
                    if (checkPendingDbSet != null && dbId != null) {
                        checkPendingDbSet.add(dbId);
                    }
                    /* Count utilization for obsolete entry. */
                    reader.countObsolete();
                    continue;
                }

                /*
                 * If we are only calculating utilization, do not process
                 * non-obsolete entries.
                 */
                if (calcUtilizationOnly) {
                    continue;
                }

                /* Evict before processing each entry. */
                if (Cleaner.DO_CRITICAL_EVICTION) {
                    env.daemonEviction(true /*backgroundIO*/);
                }

                /* The entry is not known to be obsolete -- process it now. */
                if (isLN) {

                    final LNLogEntry lnEntry = reader.getLNLogEntry();
                    lnEntry.postFetchInit(db);
                    final LN targetLN = lnEntry.getLN();
                    final byte[] key = lnEntry.getKey();

                    lookAheadCache.add
                        (Long.valueOf(DbLsn.getFileOffset(logLsn)),
                         new LNInfo(targetLN, dbId, key));

                    if (lookAheadCache.isFull()) {
                        processLN(fileNum, location, lookAheadCache, dbCache);
                    }

                    /*
                     * Process pending LNs before proceeding in order to
                     * prevent the pending list from growing too large.
                     */
                    nProcessedLNs += 1;
                    if (nProcessedLNs % PROCESS_PENDING_EVERY_N_LNS == 0) {
                        cleaner.processPending();
                    }

                } else if (isIN) {

                    final IN targetIN = reader.getIN(db);
                    targetIN.setDatabase(db);

                    processIN(targetIN, db, logLsn);

                } else if (isBINDelta) {

                    final BINDelta delta = reader.getBINDelta();
                    processBINDelta(delta, db, logLsn);

                } else if (isDbTree) {

                    env.rewriteMapTreeRoot(logLsn);
                } else {
                    assert false;
                }
            }

            /* Process remaining queued LNs. */
            if (lookAheadCache != null) {
                while (!lookAheadCache.isEmpty()) {
                    if (Cleaner.DO_CRITICAL_EVICTION) {
                        env.daemonEviction(true /*backgroundIO*/);
                    }
                    processLN(fileNum, location, lookAheadCache, dbCache);
                    /* Sleep if background read/write limit was exceeded. */
                    env.sleepAfterBackgroundIO();
                }
            }

            /* Update the pending DB set. */
            if (checkPendingDbSet != null) {
                for (Iterator<DatabaseId> i = checkPendingDbSet.iterator();
                     i.hasNext();) {
                    final DatabaseId pendingDbId = i.next();
                    final DatabaseImpl db = dbMapTree.getDb
                        (pendingDbId, cleaner.lockTimeout, dbCache);
                    cleaner.addPendingDB(db);
                }
            }

            /* Update reader stats. */
            nEntriesReadThisRun = reader.getNumRead();
            nRepeatIteratorReadsThisRun = reader.getNRepeatIteratorReads();
        } catch (ChecksumException e) {
            throw new EnvironmentFailureException
                (env, EnvironmentFailureReason.LOG_CHECKSUM, e);
        } finally {
            /* Subtract the overhead of this method from the budget. */
            budget.updateAdminMemoryUsage(0 - adjustMem);

            /* Release all cached DBs. */
            dbMapTree.releaseDbs(dbCache);
        }

        /* File is fully processed, update status information. */
        if (!calcUtilizationOnly) {
            fileSelector.addCleanedFile(fileNum, databases,
                                        reader.getLastVLSN(), budget);
        }

        return true;
    }

    /**
     * Unit testing.  Simulates processing of a single LN.
     */
    void testProcessLN(LN targetLN,
                       long logLsn,
                       byte[] key,
                       DatabaseId dbId,
                       Map<DatabaseId, DatabaseImpl> dbCache) {

        LookAheadCache lookAheadCache = new LookAheadCache(1);

        lookAheadCache.add
            (Long.valueOf(DbLsn.getFileOffset(logLsn)),
             new LNInfo(targetLN, dbId, key));

        processLN(DbLsn.getFileNumber(logLsn), new TreeLocation(),
                  lookAheadCache, dbCache);
    }

    /**
     * Processes the first LN in the look ahead cache and removes it from the
     * cache.  While the BIN is latched, look through the BIN for other LNs in
     * the cache; if any match, process them to avoid a tree search later.
     */
    private void processLN(Long fileNum,
                           TreeLocation location,
                           LookAheadCache lookAheadCache,
                           Map<DatabaseId, DatabaseImpl> dbCache)
        throws DatabaseException {

        nLNsCleanedThisRun++;

        /* Get the first LN from the queue. */
        Long offset = lookAheadCache.nextOffset();
        LNInfo info = lookAheadCache.remove(offset);

        LN ln = info.getLN();
        byte[] key = info.getKey();

        long logLsn = DbLsn.makeLsn
            (fileNum.longValue(), offset.longValue());

        /*
         * Do not call releaseDb after this getDb, since the entire dbCache
         * will be released later.
         */
        DatabaseImpl db = env.getDbTree().getDb
            (info.getDbId(), cleaner.lockTimeout, dbCache);

        /* Status variables are used to generate debug tracing info. */
        boolean processedHere = true; // The LN was cleaned here.
        boolean obsolete = false;     // The LN is no longer in use.
        boolean completed = false;    // This method completed.

        BIN bin = null;
        try {
            Tree tree = db.getTree();
            assert tree != null;

            /* Find parent of this LN. */
            boolean parentFound = tree.getParentBINForChildLN
                (location, key, false /*splitsAllowed*/,
                 true /*findDeletedEntries*/, Cleaner.UPDATE_GENERATION);
            bin = location.bin;
            int index = location.index;

            if (!parentFound) {

                nLNsDeadThisRun++;
                obsolete = true;
                completed = true;
                return;
            }

            /*
             * Now we're at the BIN parent for this LN.  If knownDeleted, LN is
             * deleted and can be purged.
             */
            if (bin.isEntryKnownDeleted(index)) {
                nLNsDeadThisRun++;
                obsolete = true;
                completed = true;
                return;
            }

            /* Process this LN that was found in the tree. */
            processedHere = false;
            processFoundLN(info, logLsn, bin.getLsn(index), bin, index);
            completed = true;

            /*
             * For all other non-deleted LNs in this BIN, lookup their LSN
             * in the LN queue and process any matches.
             */
            for (int i = 0; i < bin.getNEntries(); i += 1) {
                long binLsn = bin.getLsn(i);
                if (i != index &&
                    !bin.isEntryKnownDeleted(i) &&
                    !bin.isEntryPendingDeleted(i) &&
                    DbLsn.getFileNumber(binLsn) == fileNum.longValue()) {

                    Long myOffset = Long.valueOf(DbLsn.getFileOffset(binLsn));
                    LNInfo myInfo = lookAheadCache.remove(myOffset);

                    /* If the offset is in the cache, it's a match. */
                    if (myInfo != null) {
                        nLNQueueHitsThisRun++;
                        nLNsCleanedThisRun++;
                        processFoundLN(myInfo, binLsn, binLsn, bin, i);
                    }
                }
            }
            return;

        } finally {
            if (bin != null) {
                bin.releaseLatch();
            }

            if (processedHere) {
                cleaner.logFine(Cleaner.CLEAN_LN, ln, logLsn,
                                completed, obsolete, false /*migrated*/);
            }
        }
    }

    /**
     * Processes an LN that was found in the tree.  Lock the LN's LSN and
     * then set the entry's MIGRATE flag if the LSN of the LN log entry is the
     * active LSN in the tree.
     *
     * @param info identifies the LN log entry.
     *
     * @param logLsn is the LSN of the log entry.
     *
     * @param treeLsn is the LSN found in the tree.
     *
     * @param bin is the BIN found in the tree; is latched on method entry and
     * exit.
     *
     * @param index is the BIN index found in the tree.
     */
    private void processFoundLN(LNInfo info,
                                long logLsn,
                                long treeLsn,
                                BIN bin,
                                int index)
        throws DatabaseException {

        LN lnFromLog = info.getLN();
        byte[] key = info.getKey();

        DatabaseImpl db = bin.getDatabase();
        boolean isTemporary = db.isTemporary();

        /* Status variables are used to generate debug tracing info. */
        boolean obsolete = false;  // The LN is no longer in use.
        boolean migrated = false;  // The LN was in use and is migrated.
        boolean lockDenied = false;// The LN lock was denied.
        boolean completed = false; // This method completed.

        BasicLocker locker = null;
        try {
            Tree tree = db.getTree();
            assert tree != null;

            /*
             * If the tree and log LSNs are equal, then we can be fairly
             * certain that the log entry is current; in that case, it is
             * wasteful to lock the LN here if we will perform lazy migration
             * -- it is better to lock only once during lazy migration.  But if
             * the tree and log LSNs differ, it is likely that another thread
             * has updated or deleted the LN and the log LSN is now obsolete;
             * in this case we can avoid dirtying the BIN by checking for
             * obsoleteness here, which requires locking.  The latter case can
             * occur frequently if trackDetail is false.
             *
             * 1. If the LSN in the tree and in the log are the same, we will
             * attempt to migrate it.
             *
             * 2. If the LSN in the tree is < the LSN in the log, the log entry
             * is obsolete, because this LN has been rolled back to a previous
             * version by a txn that aborted.
             *
             * 3. If the LSN in the tree is > the LSN in the log, the log entry
             * is obsolete, because the LN was advanced forward by some
             * now-committed txn.
             *
             * 4. If the LSN in the tree is a null LSN, the log entry is
             * obsolete. A slot can only have a null LSN if the record has
             * never been written to disk in a deferred write database, and
             * in that case the log entry must be for a past, deleted version
             * of that record.
             */
            if (lnFromLog.isDeleted() &&
                (treeLsn == logLsn) &&
                fileLogVersion <= 2) {

                /*
                 * SR 14583: After JE 2.0, deleted LNs are never found in the
                 * tree, since we can assume they're obsolete and correctly
                 * marked as such in the obsolete offset tracking. JE 1.7.1 and
                 * earlier did not use the pending deleted bit, so deleted LNs
                 * may still be reachable through their BIN parents.
                 */
                obsolete = true;
                nLNsDeadThisRun++;
                bin.setPendingDeleted(index);
            } else if (treeLsn == DbLsn.NULL_LSN) {

                /*
                 * Case 4: The LN in the tree is a never-written LN for a
                 * deferred-write db, so the LN in the file is obsolete.
                 */
                obsolete = true;
            } else if (treeLsn != logLsn && isTemporary) {

                /*
                 * Temporary databases are always non-transactional.  If the
                 * tree and log LSNs are different then we know that the logLsn
                 * is obsolete.  Even if the LN is locked, the tree cannot be
                 * restored to the logLsn because no abort is possible without
                 * a transaction.  We should consider a similar optimization in
                 * the future for non-transactional durable databases.
                 */
                nLNsDeadThisRun++;
                obsolete = true;
            } else if ((treeLsn != logLsn || !cleaner.lazyMigration) &&
                       !isTemporary) {

                /*
                 * Get a lock on the LN if the treeLsn and logLsn are different
                 * to determine definitively whether the logLsn is obsolete.
                 * We must also get a lock if we will migrate the LN now
                 * (lazyMigration is false and isTemporary is false).
                 *
                 * We can hold the latch on the BIN since we always attempt to
                 * acquire a non-blocking read lock.
                 */
                locker = BasicLocker.createBasicLocker(env, false /*noWait*/);
                /* Don't allow this short-lived lock to be preempted/stolen. */
                locker.setPreemptable(false);
                LockResult lockRet = locker.nonBlockingLock
                    (treeLsn, LockType.READ, false /*jumpAheadOfWaiters*/, db);
                if (lockRet.getLockGrant() == LockGrantType.DENIED) {

                    /*
                     * LN is currently locked by another Locker, so we can't
                     * assume anything about the value of the LSN in the bin.
                     */
                    nLNsLockedThisRun++;
                    lockDenied = true;
                } else if (treeLsn != logLsn) {
                    /* The LN is obsolete and can be purged. */
                    nLNsDeadThisRun++;
                    obsolete = true;
                }
            }

            /*
             * At this point either obsolete==true, lockDenied==true, or
             * treeLsn==logLsn.
             */
            if (!obsolete && !lockDenied) {
                assert treeLsn == logLsn;

                /*
                 * If lazyMigration is true, set the migrate flag and dirty
                 * the parent IN.  The evictor or checkpointer will migrate the
                 * LN later.  If lazyMigration is false, migrate the LN now.
                 *
                 * We have a lock on the LN if we are going to migrate it now,
                 * but not if we will set the migrate flag.
                 *
                 * When setting the migrate flag, also populate the target node
                 * so it does not have to be fetched when it is migrated, if
                 * the tree and log LSNs are equal and the target is not
                 * resident.  We must call postFetchInit to initialize MapLNs
                 * that have not been fully initialized yet [#13191].
                 *
                 * For temporary databases, do not rely on the LN migration
                 * mechanism because temporary databases are not checkpointed
                 * or recovered.  Instead, dirty the LN to ensure it is
                 * flushed before its parent is written.  Because we do not
                 * attempt to lock temporary database LNs (see above) we know
                 * that if it is non-obsolete, the tree and log LSNs are equal.
                 * We will always restore the LN to the BIN slot here, and
                 * always log the dirty LN when logging the BIN.
                 *
                 * Also for temporary databases, make both the target LN and
                 * the BIN or IN parent dirty. Otherwise, when the BIN or IN is
                 * evicted in the future, it will be written to disk without
                 * flushing its dirty, migrated LNs.  [#18227]
                 */
                if (bin.getTarget(index) == null) {
                    lnFromLog.postFetchInit(db, logLsn);
                    /* Ensure keys are transactionally correct. [#15704] */
                    bin.updateNode(index, lnFromLog, key /*lnSlotKey*/);
                }

                if (isTemporary) {
                    ((LN) bin.getTarget(index)).setDirty();
                    bin.setDirty(true);
                    nLNsMarkedThisRun++;
                } else if (cleaner.lazyMigration) {
                    bin.setMigrate(index, true);
                    bin.setDirty(true);
                    nLNsMarkedThisRun++;
                } else {
                    LN targetLn = (LN) bin.getTarget(index);
                    assert targetLn != null;
                    long newLNLsn = targetLn.log
                        (env, db, bin.getKey(index), logLsn,
                         true /*backgroundIO*/,
                         Cleaner.getMigrationRepContext(targetLn));
                    bin.updateEntry(index, newLNLsn);
                    /* Evict LN if we populated it with the log LN. */
                    if (lnFromLog == targetLn) {
                        bin.updateNode(index, null, null);
                    }
                    /* Lock new LSN on behalf of existing lockers. */
                    CursorImpl.lockAfterLsnChange
                        (db, logLsn, newLNLsn, locker /*excludeLocker*/);
                    nLNsMigratedThisRun++;
                }

                /*
                 * If the generation is zero, we fetched this BIN just for
                 * cleaning.
                 */
                if (PROHIBIT_DELTAS_WHEN_FETCHING &&
                    bin.getGeneration() == 0) {
                    bin.setProhibitNextDelta();
                }

                /*
                 * Update the generation so that the BIN is not evicted
                 * immediately.  This allows the cleaner to fill in as many
                 * entries as possible before eviction, as to-be-cleaned
                 * files are processed.
                 */
                bin.setGeneration(CacheMode.DEFAULT);

                migrated = true;
            }
            completed = true;
        } finally {
            if (locker != null) {
                locker.operationEnd();
            }

            /*
             * If a write lock is held, it is likely that the log LSN will
             * become obsolete.  It is more efficient to process this via the
             * pending list than to set the MIGRATE flag, dirty the BIN, and
             * cause the BIN to be logged unnecessarily.
             */
            if (completed && lockDenied) {
                assert !isTemporary;

                /*
                 * We could associate the pending LN with either treeLsn or
                 * logLsn, we just need a convention to follow.  When
                 * processing the pending LN, we always check to see if the LSN
                 * has changed after we latch the BIN.  For consistency with
                 * other places that call addPendingLN, we use treeLsn here.
                 */
                fileSelector.addPendingLN(treeLsn, lnFromLog, db.getId(), key);
            }

            cleaner.logFine(Cleaner.CLEAN_LN, lnFromLog, logLsn, completed,
                            obsolete, migrated);
        }
    }

    /**
     * If this BINDelta is still in use in the in-memory tree, dirty the
     * associated BIN. The next checkpoint will log a new delta or a full
     * version, which will make this delta obsolete.
     */
    private void processBINDelta(BINDelta deltaClone,
                                 DatabaseImpl db,
                                 long logLsn) {

        nBINDeltasCleanedThisRun++;

        /*
         * Search Btree for the BIN associated with this delta.
         *
         * A potential optimization is to use a by-level search to find the
         * BIN's parent (getParentINForChildIN may need to be enhanced to do
         * this), then fetch the delta, then reconstitute BIN using the inClone
         * if indeed it is a delta and its lastFullVersion is LTE the log LSN.
         * This avoids fetching a BIN that does not need migration.
         */
        final byte[] searchKey = deltaClone.getSearchKey();
        final BIN treeBin = (BIN) db.getTree().search
            (searchKey, Tree.SearchType.NORMAL, null /*binBoundary*/,
             Cleaner.UPDATE_GENERATION, null /*searchComparator*/);

        if (treeBin == null) {
            /* BIN for this delta is no longer in the tree. */
            nBINDeltasDeadThisRun++;
            return;
        }

        /* Tree BIN is non-null and latched. */
        try {
            final long treeLsn = treeBin.getLastLoggedVersion();

            if (treeLsn == DbLsn.NULL_LSN) {
                /* Current version was never logged. */
                nBINDeltasDeadThisRun++;
                return;
            }

            final int cmp = DbLsn.compareTo(treeLsn, logLsn);

            if (cmp > 0) {
                /* Log entry is obsolete. */
                nBINDeltasDeadThisRun++;
                return;
            }

            /*
             * Log entry is same or newer than what's in the tree.  Dirty the
             * BIN and let the checkpoint write it out.  There is no need to
             * prohibit a delta when the BIN is next logged (as is done when
             * migrating full INs) because logging a new delta will obsolete
             * this delta.
             */
            treeBin.setDirty(true);
            nBINDeltasMigratedThisRun++;

        } finally {
            treeBin.releaseLatch();
        }
    }

    /**
     * If an IN is still in use in the in-memory tree, dirty it. The checkpoint
     * invoked at the end of the cleaning run will end up rewriting it.
     */
    private void processIN(IN inClone,
                           DatabaseImpl db,
                           long logLsn)
        throws DatabaseException {

        boolean obsolete = false;
        boolean dirtied = false;
        boolean completed = false;

        try {
            nINsCleanedThisRun++;

            Tree tree = db.getTree();
            assert tree != null;

            IN inInTree = findINInTree(tree, db, inClone, logLsn);

            if (inInTree == null) {
                /* IN is no longer in the tree.  Do nothing. */
                nINsDeadThisRun++;
                obsolete = true;
            } else {

                /*
                 * IN is still in the tree.  Dirty it.  Checkpoint or eviction
                 * will write it out.  Prohibit the next delta, since the
                 * original version must be made obsolete.
                 */
                nINsMigratedThisRun++;
                inInTree.setDirty(true);
                inInTree.setProhibitNextDelta();
                inInTree.releaseLatch();
                dirtied = true;
            }

            completed = true;
        } finally {
            cleaner.logFine(Cleaner.CLEAN_IN, inClone, logLsn, completed,
                            obsolete, dirtied);
        }
    }

    /**
     * Given a clone of an IN that has been taken out of the log, try to find
     * it in the tree and verify that it is the current one in the log.
     * Returns the node in the tree if it is found and it is current re: LSN's.
     * Otherwise returns null if the clone is not found in the tree or it's not
     * the latest version.  Caller is responsible for unlatching the returned
     * IN.
     */
    private IN findINInTree(Tree tree,
                            DatabaseImpl db,
                            IN inClone,
                            long logLsn)
        throws DatabaseException {

        /* Check if inClone is the root. */
        if (inClone.isDbRoot()) {
            IN rootIN = isRoot(tree, db, inClone, logLsn);
            if (rootIN == null) {

                /*
                 * inClone is a root, but no longer in use. Return now, because
                 * a call to tree.getParentNode will return something
                 * unexpected since it will try to find a parent.
                 */
                return null;
            } else {
                return rootIN;
            }
        }

        /* It's not the root.  Can we find it, and if so, is it current? */
        inClone.latch(Cleaner.UPDATE_GENERATION);
        SearchResult result = null;
        try {
            result = tree.getParentINForChildIN
                (inClone,
                 true,   // requireExactMatch
                 Cleaner.UPDATE_GENERATION,
                 inClone.getLevel(),
                 null);  // trackingList

            if (!result.exactParentFound) {
                return null;
            }

            /* Note that treeLsn may be for a BINDelta, see below. */
            long treeLsn = result.parent.getLsn(result.index);

            /*
             * The IN in the tree is a never-written IN for a DW db so the IN
             * in the file is obsolete. [#15588]
             */
            if (treeLsn == DbLsn.NULL_LSN) {
                return null;
            }

            /*
             * If tree and log LSNs are equal, then we've found the exact IN we
             * read from the log.  We know the treeLsn is not for a BINDelta,
             * because it is equal to LSN of the IN (or BIN) we read from the
             * log.  To avoid a fetch, we can place the inClone in the tree if
             * it is not already resident.
             */
            if (treeLsn == logLsn) {
                IN in = (IN) result.parent.getTarget(result.index);
                if (in == null) {
                    in = inClone;
                    in.postFetchInit(db, logLsn);
                    result.parent.updateNode
                        (result.index, in, null /*lnSlotKey*/);
                }
                in.latch(Cleaner.UPDATE_GENERATION);
                return in;
            }

            /*
             * If the tree and log LSNs are unequal, then we must get the full
             * version LSN in case the tree LSN is actually for a BINDelta.
             * The simplest way to do that is to fetch the IN in the tree.
             *
             * A potential optimization is to fetch the delta first and then
             * reconstitute the BIN using the inClone if the delta's
             * lastFullVersion is equal to the log LSN.  This avoids fetching
             * the inClone we have in hand, or a later full version that does
             * not need migration.
             */
            if (inClone.isBIN()) {

                /*
                 * getParentINForChildIN takes exclusive latches above, so we
                 * use fetchTargetWithExclusiveLatch.
                 */
                final IN in = (IN) result.parent.fetchTargetWithExclusiveLatch
                    (result.index);
                treeLsn = in.getLastFullVersion();
            }

            /* Now compare LSNs, since we know treeLsn is the full version. */
            final int compareVal = DbLsn.compareTo(treeLsn, logLsn);

            if (compareVal > 0) {
                /* Log entry is obsolete. */
                return null;
            }

            /*
             * Log entry is same or newer than what's in the tree.
             * getParentINForChildIN takes exclusive latches above, so we use
             * fetchTargetWithExclusiveLatch.
             */
            final IN in = (IN) result.parent.fetchTargetWithExclusiveLatch
                (result.index);
            in.latch(Cleaner.UPDATE_GENERATION);
            return in;
        } finally {
            if ((result != null) && (result.exactParentFound)) {
                result.parent.releaseLatch();
            }
        }
    }

    /**
     * Get the current root in the tree, or null if the inClone is not the
     * current root.
     */
    private static class RootDoWork implements WithRootLatched {
        private final DatabaseImpl db;
        private final IN inClone;
        private final long logLsn;

        RootDoWork(DatabaseImpl db, IN inClone, long logLsn) {
            this.db = db;
            this.inClone = inClone;
            this.logLsn = logLsn;
        }

        public IN doWork(ChildReference root)
            throws DatabaseException {

            if (root == null ||
                (root.getLsn() == DbLsn.NULL_LSN) || // deferred write root
                (((IN) root.fetchTarget(db, null)).getNodeId() !=
                 inClone.getNodeId())) {
                return null;
            }

            /*
             * A root LSN less than the log LSN must be an artifact of when we
             * didn't properly propagate the logging of the rootIN up to the
             * root ChildReference.  We still do this for compatibility with
             * old log versions but may be able to remove it in the future.
             */
            if (DbLsn.compareTo(root.getLsn(), logLsn) <= 0) {
                IN rootIN = (IN) root.fetchTarget(db, null);
                rootIN.latch(Cleaner.UPDATE_GENERATION);
                return rootIN;
            } else {
                return null;
            }
        }
    }

    /**
     * Check if the cloned IN is the same node as the root in tree.  Return the
     * real root if it is, null otherwise.  If non-null is returned, the
     * returned IN (the root) is latched -- caller is responsible for
     * unlatching it.
     */
    private IN isRoot(Tree tree, DatabaseImpl db, IN inClone, long lsn)
        throws DatabaseException {

        RootDoWork rdw = new RootDoWork(db, inClone, lsn);
        return tree.withRootLatchedShared(rdw);
    }

    /**
     * Reset per-run counters.
     */
    private void resetPerRunCounters() {
        nINsObsoleteThisRun = 0;
        nINsCleanedThisRun = 0;
        nINsDeadThisRun = 0;
        nINsMigratedThisRun = 0;
        nBINDeltasObsoleteThisRun = 0;
        nBINDeltasCleanedThisRun = 0;
        nBINDeltasDeadThisRun = 0;
        nBINDeltasMigratedThisRun = 0;
        nLNsObsoleteThisRun = 0;
        nLNsCleanedThisRun = 0;
        nLNsDeadThisRun = 0;
        nLNsMigratedThisRun = 0;
        nLNsMarkedThisRun = 0;
        nLNQueueHitsThisRun = 0;
        nLNsLockedThisRun = 0;
        nEntriesReadThisRun = 0;
        nRepeatIteratorReadsThisRun = 0;
    }

    /**
     * Add per-run counters to total counters.
     */
    private void accumulatePerRunCounters() {
        cleaner.nINsObsolete.add(nINsObsoleteThisRun);
        cleaner.nINsCleaned.add(nINsCleanedThisRun);
        cleaner.nINsDead.add(nINsDeadThisRun);
        cleaner.nINsMigrated.add(nINsMigratedThisRun);
        cleaner.nBINDeltasObsolete.add(nBINDeltasObsoleteThisRun);
        cleaner.nBINDeltasCleaned.add(nBINDeltasCleanedThisRun);
        cleaner.nBINDeltasDead.add(nBINDeltasDeadThisRun);
        cleaner.nBINDeltasMigrated.add(nBINDeltasMigratedThisRun);
        cleaner.nLNsObsolete.add(nLNsObsoleteThisRun);
        cleaner.nLNsCleaned.add(nLNsCleanedThisRun);
        cleaner.nLNsDead.add(nLNsDeadThisRun);
        cleaner.nLNsMigrated.add(nLNsMigratedThisRun);
        cleaner.nLNsMarked.add(nLNsMarkedThisRun);
        cleaner.nLNQueueHits.add(nLNQueueHitsThisRun);
        cleaner.nLNsLocked.add(nLNsLockedThisRun);
        cleaner.nRepeatIteratorReads.add(nRepeatIteratorReadsThisRun);
    }

    /**
     * A cache of LNInfo by LSN offset.  Used to hold a set of LNs that are
     * to be processed.  Keeps track of memory used, and when full (over
     * budget) the next offset should be queried and removed.
     */
    private static class LookAheadCache {

        private final SortedMap<Long,LNInfo> map;
        private final int maxMem;
        private int usedMem;

        LookAheadCache(int lookAheadCacheSize) {
            map = new TreeMap<Long,LNInfo>();
            maxMem = lookAheadCacheSize;
            usedMem = MemoryBudget.TREEMAP_OVERHEAD;
        }

        boolean isEmpty() {
            return map.isEmpty();
        }

        boolean isFull() {
            return usedMem >= maxMem;
        }

        Long nextOffset() {
            return map.firstKey();
        }

        void add(Long lsnOffset, LNInfo info) {
            map.put(lsnOffset, info);
            usedMem += info.getMemorySize();
            usedMem += MemoryBudget.TREEMAP_ENTRY_OVERHEAD;
        }

        LNInfo remove(Long offset) {
            LNInfo info = map.remove(offset);
            if (info != null) {
                usedMem -= info.getMemorySize();
                usedMem -= MemoryBudget.TREEMAP_ENTRY_OVERHEAD;
            }
            return info;
        }
    }
}
