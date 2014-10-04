/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2010 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.cleaner;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Contains methods for calculating utilization and for selecting files to
 * clean.
 *
 * In most cases the methods in this class are called by FileSelector methods,
 * and synchronization order is always FileSelector then UtilizationCalculator.
 * In some cases methods in this class are called directly by FileProcessor,
 * and such methods must take care not to call FileSelector methods.
 *
 * This class maintains the size correction factor for LNs whose size is not
 * counted, and related info, as explained in detail below. [#18633]
 *
 * (Historical note:  Originally in JE 5, the corrected average LN size was
 * used to adjust utilization.  This was changed to a correction factor since
 * different log files may have different average LN sizes. [#21106])
 *
 * LN size correction is necessary because the obsolete size of LNs may be
 * unknown when we delete or update an LN that is not resident in cache.  We do
 * not wish to fetch the LN just to obtain the size of the previous version.
 * Therefore we record in the FileSummary that an LN became obsolete (by
 * incrementing obsoleteLNCount) and also that its size was not counted (by not
 * incrementing obsoleteLNSizeCounted and not adding a size to obsoleteLNSize).
 * 
 * Later we estimate utilization by assuming that such obsolete LNs have sizes
 * that are roughly the average LN size for obsolete LNs whose size was not
 * counted in the file, as calculated by FileSummary.getObsoleteLNSize and
 * getAverageObsoleteLNSizeNotCounted.  This is fairly accurate when the sizes
 * of obsolete and non-obsolete LNs in a file are uniformly distributed.  But
 * it can be inaccurate when they are not.
 *
 * If obsolete LNs are generally smaller than non-obsolete LNs, the average
 * will be larger than the true size and estimated utilization will be lower
 * then true utilization; in this case there is danger of over-cleaning and
 * wasted resources.  If obsolete LNs are generally larger, the average will be
 * smaller than the true size and estimated utilization will be higher than
 * true utilization; in this case there is danger of unbounded log growth and a
 * "disk space leak".
 * 
 * To correct the inaccurate estimate we take advantage of the fact that the
 * true LN sizes and true average LN size can be determined when the
 * FileProcessor reads a log file.  We save the true average LN size as the
 * result of a FileProcessor run in the adjustUtilization method.  The true
 * average LN size is then used to calculate a correction factor -- a factor
 * that, when multiplied by the estimated average LN size, results in the
 * true average LN size.  The correction factor is used when determining the
 * utilization of other log files; this is done by the getBestFile method.
 *
 * To guard against over-cleaning (the first case described above), the
 * FileProcessor will naturally select a file for cleaning because estimated
 * utilization is lower than true utilization.  When the file is cleaned, the
 * LN size correction factor will be determined.  However, to guard against
 * unbounded log growth (the second case above), this mechanism is not
 * sufficient because estimated utilization is higher than true utilization and
 * a file may not be selected for cleaning.  To handle this case, a log file
 * must sometimes be read by the FileProcessor, even though estimated
 * utilization is above the minUtilization threshold, to "probe" the true
 * utilization and determine the true average LN size.  Probing is a read-only
 * operation and does not migrate the active LNs in the log file or attempt to
 * delete the file.
 *
 * Probing a log file is extra work and we strive to do it as infrequently as
 * possible.  The following algorithm is used.
 *
 * 1) We determine the number of new log files N that have been written since
 * the last time adjustUtilization was called.  Recall that adjustUtilization
 * is called when FileProcessor reads a file, for cleaning a file or probing
 * it.  N is determined by the shouldPerformProbe method, which is called via
 * FileSelector.selectFileForCorrection by the FileProcessor when the
 * FileProcessor is woken to do cleaning but no file is selected for cleaning.
 *
 * 2) shouldPerformProbe returns true only if N is greater than a threshold
 * value.  The threshold has a high value (20 by default) if the minimum number
 * of initial adjustments (files cleaned or probed, 5 by default) has not been
 * reached, and has a lower value (5 by default) after the initial adjustment
 * minimum has been reached.  In other words, we do probes more frequently when
 * we haven't collected a lot of input data.
 *
 * 3) If shouldPerformProbe returns true, then selectFileForCorrection
 * in the FileSelector calls the getBestFile method here, passing true for the
 * isProbe argument.  In this mode, getBestFile will select a file based on
 * worst case (lowest possible) utilization.  The maximum LN size in the file
 * is used, rather than the average LN size, for this worst case calculation.
 * See the FileSummary getMaxObsoleteLNSize method.  If no file is selected
 * using worst case utilization, then probing is not needed because total
 * utilization cannot be lower than the configured minUtilization.
 *
 * 4) If FileSelector.selectFileForCorrection returns a non-null file number,
 * then FileProcessor will read the file in calcUtilizationOnly mode.  True
 * utilization will be determined and adjustUtilization will be called, but
 * active LNs will not be migrated and the file will not be deleted.
 *
 * To summarize, a file is read in probing mode only if both of the following
 * conditions are satisfied:
 *
 *  a) The number of new files written since the last correction, N, is greater
 *     than the threshold, which is 5 or 20 depending on the whether the last
 *     correction is greater or less than 0.9.
 *
 *  b) Using a worst case utilization calculation that uses the maximum
 *     obsolete LN size in a file rather than the average, the total log
 *     utilization is under the configured minUtilization.
 *
 * To determine the LN size correction factor that is applied when estimating
 * utilization, rather than using the size for a single log file, the sizes for
 * the last several log files are saved and the average value is used.  The
 * average over the last 10 adjustments is computed when adjustUtilization is
 * called.
 *
 * If the mix of LN sizes for obsolete and non-obsolete LNs is roughly the same
 * for all log files, and this mix does not change over time, then using a
 * running average would not be necessary and a single size would be
 * sufficient.  However, some applications have access patterns that are not
 * uniform over time, and the running average size will help to compensate for
 * this.
 *
 * It is important to avoid premature use of the LN size correction factor in
 * selecting files for cleaning, before enough input data has been collected to
 * make the computed value meaningful.  To accomplish this, the correction
 * factor is not computed or used until the average size data for a minimum
 * number of LNs with uncounted sizes (1000 by default) is available.
 *
 * Because it is expensive to compute, the recently computed values are stored
 * persistently as part of the CheckpointEnd log entry.  Flushing this info
 * once per checkpoint is sufficient, and no special handling is necessary
 * after a crash; losing the new info since the last checkpoint is acceptable.
 */
public class UtilizationCalculator {

    /**
     * Whether to clean files protected from deletion by HA/DataSync.
     * Pros:
     * + If a large number of files were to become unprotected at once, a large
     *   amount of log cleaning may suddenly be necessary.  Cleaning the files
     *   avoids this.  Better still would be to delete the metadata, but that
     *   would require writing a log entry to indicate the file is ready to be
     *   deleted, to avoid cleaning from scratch after a crash.
     * Cons:
     * - When we clean protected files that would otherwise contain a portion
     *   of utilized entries, the utilized entries that are migrated are
     *   duplicated on disk.  In other words we use more disk space and the
     *   chance of filling the disk is higher.
     * - Unlike the files protected by a DbBackup, files may be protected
     *   by HA for an extended period when a node is down (the maximum interval
     *   is user configurable) or by DataSync when the external system is down.
     * - It may be wasteful to clean a file before it can be deleted.  The
     *   file will normally become obsolete naturally over time, saving
     *   the work of migrating log entries during log cleaning later.
     * - If we clean files we cannot delete, the backlog will consume
     *   memory in the FileSelector.  By not cleaning them, we guard
     *   against excessive memory usage for a large cleaner backlog, when
     *   many files are protected.  (However, all existing files consume
     *   memory in the UtilizationProfile and DatabaseImpl.)
     *
     * Files protected by DbBackup are not impacted by this setting.  They are
     * always cleaned because we can assume the protection is for a short
     * duration.
     *
     * Currently set to true. [#16643] [#19221]
     */
    private static final boolean CLEAN_HA_DATASYNC_PROTECTED_FILES = true;

    private static final boolean DEBUG = false;

    /*
     * Configuration properties.  The default values are probably appropriate
     * for all applications and these parameters are currently not exposed in
     * EnvironmentConfig.  However, in case exposing them or experimenting with
     * them is necessary in the future, they are represented as hidden
     * properties in the EnvironmentParams class.
     */
    private final int configRecentLNSizes;      // default: 10
    private final int configMinUncountedLNs;    // default: 1000
    private final int configInitialAdjustments; // default: 5
    private final int configMinProbeSkipFiles;  // default: 5
    private final int configMaxProbeSkipFiles;  // default: 20

    private final EnvironmentImpl env;
    private final Cleaner cleaner;
    private final Logger logger;
    private final FilesToMigrate filesToMigrate;
    private long minProtectedFile;
    private float lnSizeCorrectionFactor;
    private final LinkedList<AverageSize> recentAvgLNSizes;
    private long endFileNumAtLastAdjustment;
    private int initialAdjustments;
    private TestHook<TestAdjustment> adjustmentHook;

    UtilizationCalculator(EnvironmentImpl env, Cleaner cleaner) {
        this.env = env;
        this.cleaner = cleaner;
        logger = LoggerUtils.getLogger(getClass());

        /*
         * Set default values. Some are overridden by setLogSummary when
         * opening an existing environment.
         */
        lnSizeCorrectionFactor = Float.NaN;
        recentAvgLNSizes = new LinkedList<AverageSize>();
        endFileNumAtLastAdjustment = -1;
        minProtectedFile = Long.MAX_VALUE;
        filesToMigrate = new FilesToMigrate(env);

        /* Configuration properties.  All are immutable. */
        final DbConfigManager configManager = env.getConfigManager();
        configRecentLNSizes = configManager.getInt
            (EnvironmentParams.CLEANER_CALC_RECENT_LN_SIZES);
        configMinUncountedLNs = configManager.getInt
            (EnvironmentParams.CLEANER_CALC_MIN_UNCOUNTED_LNS);
        configInitialAdjustments = configManager.getInt
            (EnvironmentParams.CLEANER_CALC_INITIAL_ADJUSTMENTS);
        configMinProbeSkipFiles = configManager.getInt
            (EnvironmentParams.CLEANER_CALC_MIN_PROBE_SKIP_FILES);
        configMaxProbeSkipFiles = configManager.getInt
            (EnvironmentParams.CLEANER_CALC_MAX_PROBE_SKIP_FILES);
    }

    /**
     * Returns log summary info that should be saved persistently.
     */
    public synchronized CleanerLogSummary getLogSummary() {
        return new CleanerLogSummary(new ArrayList(recentAvgLNSizes),
                                     endFileNumAtLastAdjustment,
                                     initialAdjustments);
    }

    /**
     * Restores log summary info that was read from persistent storage.
     */
    public synchronized void setLogSummary(CleanerLogSummary logSummary) {
        endFileNumAtLastAdjustment =
            logSummary.getEndFileNumAtLastAdjustment();
        initialAdjustments = logSummary.getInitialAdjustments();
        recentAvgLNSizes.clear();
        recentAvgLNSizes.addAll(logSummary.getRecentAvgLNSizes());
        /* Call this last, as it uses the fields set above. */
        updateObsoleteLNSizeCorrectionFactor();
    }

    /**
     * See UtilizationCorrectionTest.
     */
    synchronized void setAdjustmentHook(TestHook<TestAdjustment> hook) {
        adjustmentHook = hook;
    }

    /**
     * Determine which files are protectd from deletion, which influences which
     * files are cleaned.
     *
     * This method must not be called while synchronized on FileSelector.
     * RepImpl.getCleanerBarrierStartFile does cursor operations and this would
     * cause a deadlock.
     */
    synchronized void setProtectedFiles() {
        if (!CLEAN_HA_DATASYNC_PROTECTED_FILES) {
            minProtectedFile = env.getCleanerBarrierStartFile();
        }
    }

    /**
     * Returns the factor to be multiplied by the average LN size (for LNs with
     * uncounted sizes) to correct for differences between obsolete and active
     * LN sizes.  This method is intentionally unsynchronized to provide fast
     * access for stats.
     */
    public float getLNSizeCorrectionFactor() {
        return lnSizeCorrectionFactor;
    }

    /**
     * Returns the best file that qualifies for cleaning or probing, or null
     * if no file qualifies.
     *
     * @param fileSummaryMap the map containing file summary info.
     *
     * @param forceCleaning is true to always select a file, even if its
     * utilization is above the minimum utilization threshold.
     *
     * @param lowUtilizationFiles is a returned set of files that are below the
     * minimum utilization threshold.
     *
     * @param isBacklog is true if there is currently a backlog, in which case
     * FilesToMigrate won't be used to return a file.
     *
     * @param isProbe is true to use the maximum LN obsolete size to determine
     * file utilization.  It should be false when selecting a file cleaning a
     * file normally, to use the average LN size for uncounted sizes along with
     * correction factor.  It should be true when selecting a file to calculate
     * utilization without cleaning, to determine the worst case (lowest
     * possible) utilization and to ignore the correction factor.
     */
    synchronized Long
        getBestFile(SortedMap<Long, FileSummary> fileSummaryMap,
                    boolean forceCleaning,
                    Set<Long> lowUtilizationFiles,
                    boolean isBacklog,
                    boolean isProbe) {
        if ((!CLEAN_HA_DATASYNC_PROTECTED_FILES) && (minProtectedFile == -1)) {

            /* 
             * The replicated node is not available, so the cleaner barrier
             * cannot be read. Don't clean any files.
             */
            LoggerUtils.logMsg(logger, env, Level.SEVERE,
                               "Can't clean, HA protects all files.");
            return null;
        }

        /* Start with an empty set.*/
        if (lowUtilizationFiles != null) {
            lowUtilizationFiles.clear();
        }

        /* Paranoia.  There should always be 1 file. */
        if (fileSummaryMap.size() == 0) {
            LoggerUtils.logMsg(logger, env, Level.SEVERE,
                               "Can't clean, map is empty.");
            return null;
        }

        /* Used to avoid checking for Level.FINE on every iteration. */
        final boolean isLoggingLevelFine = logger.isLoggable(Level.FINE);

        /*
         * Use local variables for mutable properties.  Using values that are
         * changing during a single file selection pass would not produce a
         * well defined result.
         *
         * Note that age is a distance between files not a number of files,
         * that is, deleted files are counted in the age.
         */
        final int useMinUtilization = cleaner.minUtilization;
        final int useMinFileUtilization = cleaner.minFileUtilization;
        final int useMinAge = cleaner.minAge;

        /*
         * Cleaning must refrain from rearranging the portion of log processed
         * as recovery time. Do not clean a file greater or equal to the first
         * active file used in recovery, which is either the last log file or
         * the file of the first active LSN in an active transaction, whichever
         * is earlier.
         *
         * TxnManager.getFirstActiveLsn() (firstActiveTxnLsn below) is
         * guaranteed to be earlier or equal to the first active LSN of the
         * checkpoint that will be performed before deleting the selected log
         * file. By selecting a file prior to this point we ensure that will
         * not clean any entry that may be replayed by recovery.
         *
         * For example:
         * 200 ckptA start, determines that ckpt's firstActiveLsn = 100
         * 400 ckptA end
         * 600 ckptB start, determines that ckpt's firstActiveLsn = 300
         * 800 ckptB end
         *
         * Any cleaning that executes before ckpt A start will be constrained
         * to files <= lsn 100, because it will have checked the TxnManager.
         * If cleaning executes after ckptA start, it may indeed clean after
         * ckptA's firstActiveLsn, but the cleaning run will wait to ckptB end
         * to delete files.
         */
        long firstActiveFile = fileSummaryMap.lastKey().longValue();
        final long firstActiveTxnLsn = env.getTxnManager().getFirstActiveLsn();
        if (firstActiveTxnLsn != DbLsn.NULL_LSN) {
            long firstActiveTxnFile = 
                DbLsn.getFileNumber(firstActiveTxnLsn);
            if (firstActiveFile > firstActiveTxnFile) {
                firstActiveFile = firstActiveTxnFile;
            }
        }

        /*
         * Note that minAge is at least one and may be configured to a higher
         * value to prevent cleaning recently active files.
         */
        final long lastFileToClean = firstActiveFile - useMinAge;

        /* Calculate totals and find the best file. */
        Long bestFile = null;
        int bestUtilization = 101;
        long totalSize = 0;
        long totalObsoleteSize = 0;

        for (final Map.Entry<Long, FileSummary> entry :
             fileSummaryMap.entrySet()) {
            final Long file = entry.getKey();
            final long fileNum = file.longValue();

            /*
             * If we do not clean files protected by HA, do not allow them to
             * be selected and calculate the total utilization as if they do
             * not exist.  When many files are protected and have low
             * utilization, if we were to count them in the total then we may
             * clean other unprotected files with higher utilization, which is
             * expensive.  OTOH, with the current approach we may clean more
             * unprotected files than necessary when the protected files have
             * high utilization.  Overall, counting only the files that we can
             * clean gives the best results from the selection algorithm.
             * [#16643]
             */
            if (!CLEAN_HA_DATASYNC_PROTECTED_FILES) {
                if (fileNum >= minProtectedFile) {
                    continue;
                }
            }

            final FileSummary summary = entry.getValue();
            final int obsoleteSize = isProbe ?
                summary.getMaxObsoleteSize() :
                summary.getObsoleteSize(lnSizeCorrectionFactor);

            /*
             * If the file is already being cleaned, only total the
             * non-obsolete amount.  This is an optimistic prediction of the
             * results of cleaning, and is used to prevent over-cleaning.
             */
            if (cleaner.getFileSelector().isFileCleaningInProgress(file)) {
                final int utilizedSize = summary.totalSize - obsoleteSize;
                totalSize += utilizedSize;
                if (isLoggingLevelFine) {
                    LoggerUtils.logMsg
                        (logger, env, Level.FINE,
                         "Skip file previously selected for cleaning: 0x" +
                         Long.toHexString(fileNum) + " utilizedSize: " +
                         utilizedSize + " " + summary);
                }
                continue;
            }

            /* Add this file's value to the totals. */
            totalSize += summary.totalSize;
            totalObsoleteSize += obsoleteSize;

            /* Skip files that are too young to be cleaned. */
            if (fileNum > lastFileToClean) {
                continue;
            }

            /* Select this file if it has the lowest utilization so far. */
            final int thisUtilization =
                FileSummary.utilization(obsoleteSize, summary.totalSize);
            if (bestFile == null || thisUtilization < bestUtilization) {
                bestFile = file;
                bestUtilization = thisUtilization;
            }

            /* Return all low utilization files. */
            if (lowUtilizationFiles != null &&
                thisUtilization < useMinUtilization) {
                lowUtilizationFiles.add(file);
            }
        }

        /*
         * The first priority is to clean the log up to the minimum utilization
         * level, so if we're below the minimum (or an individual file is below
         * the minimum for any file), then we clean the lowest utilization
         * (best) file.  Otherwise, if there are more files to migrate, we
         * clean the next file to be migrated.  Otherwise, if cleaning is
         * forced (for unit testing), we clean the lowest utilization file.
         */
        final Long fileChosen;
        final String loggingMsg;
        final int totalUtilization =
            FileSummary.utilization(totalObsoleteSize, totalSize);

        if (totalUtilization < useMinUtilization ||
            bestUtilization < useMinFileUtilization) {
            fileChosen = bestFile;
            loggingMsg = "Chose lowest utilized file for cleaning.";
        } else if (!isBacklog &&
                   filesToMigrate.hasNext(fileSummaryMap)) {
            fileChosen = filesToMigrate.next(fileSummaryMap);
            loggingMsg = "Chose file from files-to-migrate for cleaning.";
        } else if (forceCleaning) {
            fileChosen = bestFile;
            loggingMsg = "Chose file for forced cleaning (during testing).";
        } else {
            fileChosen = null;
            loggingMsg = "No file selected for cleaning.";
        }

        if (logger.isLoggable(Level.INFO)) {
            final String fileChosenString = (fileChosen != null) ?
                (" fileChosen: 0x" + Long.toHexString(fileChosen)) :
                "";
            LoggerUtils.logMsg
                (logger, env, Level.INFO,
                 loggingMsg + fileChosenString +
                 " totalUtilization: " + totalUtilization +
                 " bestFileUtilization: " + bestUtilization +
                 " lnSizeCorrectionFactor: " + lnSizeCorrectionFactor +
                 " isProbe: " + isProbe);
        }

        return fileChosen;
    }

    /**
     * Returns the cheapest file to clean from the given list of files.
     *
     * The cheapest file is considered to be the one with least number of
     * active (non-obsolete) entries, since each active entry requires a Btree
     * lookup and must be logged.
     * 
     * This method is used to select the first file to be cleaned in the batch
     * of to-be-cleaned files.  If there is no backlog, then this method always
     * returns the first and only file in the candidate set, so the cost
     * algorithm has no impact.
     *
     * Returns null iff the candidate set is empty.
     */
    synchronized Long
        getCheapestFileToClean(SortedMap<Long, FileSummary> fileSummaryMap,
                               SortedSet<Long> candidateFiles) {

        if (candidateFiles.size() == 0) {
            return null;
        }

        if (candidateFiles.size() == 1) {
            return candidateFiles.first();
        }

        Long bestFile = null;
        int bestCost = Integer.MAX_VALUE;

        for (final Long file : candidateFiles) {
            final FileSummary summary = fileSummaryMap.get(file);

            /*
             * Return a file in the given set if it does not exist.  Deleted
             * files should be selected ASAP to remove them from the backlog.
             * [#18179] For details, see where FileProcessor.doClean handles
             * LogFileNotFoundException.
             */
            if (summary == null) {
                return file;
            }

            /* Calculate this file's cost to clean. */
            final int thisCost = summary.getNonObsoleteCount();

            /* Select this file if it has the lowest cost so far. */
            if (bestFile == null || thisCost < bestCost) {
                bestFile = file;
                bestCost = thisCost;
            }
        }

        return bestFile;
    }

    /**
     * Saves the true average LN size for use in calculating utilization.  The
     * true FileSummary info is determined by FileProcessor when cleaning or
     * probing a file.
     *
     * The following example shows how the true average LN size (for LNs with
     * their size uncounted) is calculated, given the estimated and the true
     * FileSummary info.
     *
     * Estimated FileSummary
     * ---------------------
     * This calculation is performed by FileSummary
     * getAverageObsoleteLNSizeNotCounted and documented there.
     *
     * estimatedFileSummary.totalLNCount: 1,000
     * estimatedFileSummary.totalLNSize: 100,000
     * estimatedFileSummary.obsoleteLNCount: 500
     * estimatedFileSummary.obsoleteLNSizeCounted: 250
     * estimatedFileSummary.obsoleteLNSize: 12,500
     * average counted obsolete LN size: 12,500/250 == 50
     * remainder LN count: 1,000 - 250 == 750
     * remainder LN size: 100,000 - 12,500 = 87,500
     * average uncounted obsolete LN size: 87,500 / 750 == 116.67
     * estimated obsolete LN size: (116.67 * 250) + 12,500 == 41,667.5
     *
     * True FileSummary
     * ----------------
     * This calculation is perform in this method, given the obsoleteLNSize
     * amount in the true FileSummary and the information from the estimated
     * FileSummary above.
     *
     * trueFileSummary.obsoleteLNSize: 60,000
     *
     * average uncounted obsolete LN size:
     *  (60,000 - 12,500) / (500 - 250) == 190
     *
     * which is:
     * (trueFileSummary.obsoleteLNSize -
     *  estimatedFileSummary.obsoleteLNSize)
     *  /
     * (estimatedFileSummary.obsoleteLNCount -
     *  estimatedFileSummary.obsoleteLNSizeCounted)
     *
     * As a check, if 190 were used in the estimation further above, rather
     * than using 116.67, then the result would be 60,000:
     *
     * estimated obsolete LN size: (190 * 250) + 12,500 == 60,000
     */
    synchronized void adjustUtilization(long fileNum,
                                        long endFileNum,
                                        FileSummary estimatedFileSummary, 
                                        FileSummary trueFileSummary) {

        /* Save last file num at the time of the update. */
        endFileNumAtLastAdjustment = endFileNum;

        /*
         * Keep track of the number of adjustments, which is the number of
         * files cleaned or probed.
         */
        if (initialAdjustments < configInitialAdjustments) {
            initialAdjustments += 1;
        }

        /* Normalize obsolete amounts to account for double-counting. */
        final int estObsLNCount =
            Math.min(estimatedFileSummary.obsoleteLNCount,
                     estimatedFileSummary.totalLNCount);
        final int estObsLNSize =
            Math.min(estimatedFileSummary.obsoleteLNSize,
                     estimatedFileSummary.totalLNSize);
        final int estObsLNSizeCounted =
            Math.min(estimatedFileSummary.obsoleteLNSizeCounted,
                     estObsLNCount);
        final int trueObsLNSize =
            Math.min(trueFileSummary.obsoleteLNSize,
                     trueFileSummary.totalLNSize);

        /* Estimated average for this adjustment, used for debugging. */
        final float estimatedAvgSize = 
            estimatedFileSummary.getAvgObsoleteLNSizeNotCounted();

        /* Calculate true average as described in the method comment above. */
        final float trueAvgSize;
        final float correctionFactor;
        final int uncountedSize = trueObsLNSize - estObsLNSize;
        final int uncountedCount = estObsLNCount - estObsLNSizeCounted;

        /* If no LN sizes are uncounted, don't apply this adjustment. */
        if (uncountedSize > 0 && uncountedCount > 0) {

            /* True average size for this adjustment. */
            trueAvgSize = uncountedSize / ((float) uncountedCount);

            /* Estimated average size for this adjustment. */
            final int estUncountedSize =
                estimatedFileSummary.totalLNSize - estObsLNSize;
            final int estUncountedCount =
                estimatedFileSummary.totalLNCount - estObsLNSizeCounted;
            assert estimatedAvgSize ==
                estUncountedSize / ((float) estUncountedCount) :
                "expected=" + estimatedAvgSize +
                "got=" + (estUncountedSize / ((float) estUncountedCount));

            /* Add new average to recent values, remove oldest value. */
            recentAvgLNSizes.addLast
                (new AverageSize(uncountedSize, uncountedCount,
                                 estUncountedSize, estUncountedCount));
            while (recentAvgLNSizes.size() > configRecentLNSizes) {
                recentAvgLNSizes.removeFirst();
            }

            /* Update current correction factor. */
            updateObsoleteLNSizeCorrectionFactor();

            /* Calc this file's correction factor for testing. */
            correctionFactor = trueAvgSize / estimatedAvgSize;
        } else {
            trueAvgSize = Float.NaN;
            correctionFactor = Float.NaN;
        }

        /* Output debug info. */
        if (DEBUG) {
            System.out.println("estimatedAvgSize=" + estimatedAvgSize +
                               " trueAvgSize=" + trueAvgSize +
                               " fileNum=" + fileNum +
                               "\nestimatedFileSummary=\n" +
                               estimatedFileSummary +
                               "\ntrueFileSummary=\n" + trueFileSummary);
        }

        /* Call test hook. */
        boolean assertionsEnabled = false;
        assert (assertionsEnabled = true);
        if (assertionsEnabled && adjustmentHook != null) {
            final TestAdjustment testAdjustment = new TestAdjustment
                (fileNum, endFileNum, estimatedFileSummary, trueFileSummary,
                 estimatedAvgSize, trueAvgSize, correctionFactor);
            TestHookExecute.doHookIfSet(adjustmentHook, testAdjustment);
        }
    }
    
    /**
     * Sets lnSizeCorrectionFactor using recent average LN sizes.
     */
    private void updateObsoleteLNSizeCorrectionFactor() {

        /* Calculate totals. */
        long totalSize = 0;
        long totalCount = 0;
        long estTotalSize = 0;
        long estTotalCount = 0;

        for (AverageSize avg : recentAvgLNSizes) {
            totalSize += avg.size;
            totalCount += avg.count;
            estTotalSize += avg.estSize;
            estTotalCount += avg.estCount;
        }

        /*
         * The correction factor is updated only if the number of LNs is high
         * enough to be representative.  lnSizeCorrectionFactor may have been
         * previously calculated and will remain unchanged if the recent
         * AverageSize info does not have configMinUncountedLNs.
         */
        if (totalCount < configMinUncountedLNs) {
            return;
        }

        /*
         * The result of the following calculation is that:
         *
         *  estimatedAvgLNSize * lnSizeCorrectionFactor == correctedAvgLNSize
         *
         * which is how lnSizeCorrectionFactor is used in the FileSummary class
         * to calculate corrected LN sizes and utilization.
         */
        final float correctedAvgLNSize =
            (float) (totalSize / ((double) totalCount));

        final float estimatedAvgLNSize =
            (float) (estTotalSize / ((double) estTotalCount));

        lnSizeCorrectionFactor = correctedAvgLNSize / estimatedAvgLNSize;
    }

    /**
     * Returns whether enough adjustments have been made to conclude that the
     * the LN size correction factor has been established, or at least
     * unnecessary because very few LN sizes are uncounted.  When false is
     * returned, a backlog of to-be-cleaned files should not be created,
     * because when the correction factor is established we may find no need to
     * clean the files in the backlog.  Note that we do not clear the backlog
     * when we establish the size.
     */
    synchronized boolean isCorrectionEstablished() {
        return (initialAdjustments >= configInitialAdjustments);
    }

    /**
     * Returns whether a correction probe should be attempted, if worst case
     * utilization also indicates that cleaning may be needed.
     *
     * As a side effect whenever true is returned, we update
     * endFileNumAtLastAdjustment, which is somewhat redundant because
     * adjustUtilization will also update this field after the probe is
     * complete.  Updating it here ensures that only a single probe will be
     * performed at the scheduled interval, and that multiple cleaner threads
     * will not do a probe at the same time.  This method is synchronized to
     * guarantee that.
     */
    synchronized boolean shouldPerformProbe(long endFileNum) {

        final int filesBeforeProbe = isCorrectionEstablished() ?
            configMaxProbeSkipFiles : configMinProbeSkipFiles;

        if (endFileNum - endFileNumAtLastAdjustment < filesBeforeProbe) {
            return false;
        }

        endFileNumAtLastAdjustment = endFileNum;
        return true;
    }

    /**
     * Bundles a count of LNs and their total size, for use in calculating a
     * running average.
     */
    static class AverageSize {
        final int size;
        final int count;
        final int estSize;
        final int estCount;

        AverageSize(int size, int count, int estSize, int estCount) {
            this.size = size;
            this.count = count;
            this.estSize = estSize;
            this.estCount = estCount;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof AverageSize)) {
                return false;
            }
            final AverageSize o = (AverageSize) other;
            return size == o.size && count == o.count &&
                estSize == o.estSize && estCount == o.estCount;
        }

        @Override
        public int hashCode() {
            return size + count + estSize + estCount;
        }

        @Override
        public String toString() {
            return "size=" + size + " count=" + count +
                " estSize=" + estSize + " estCount=" + estCount;
        }
    }

    /**
     * For passing adjustment information to a test hook.
     */
    static class TestAdjustment {
        final long fileNum;
        final long endFileNum;
        final FileSummary estimatedFileSummary;
        final FileSummary trueFileSummary;
        final float estimatedAvgSize;
        final float trueAvgSize;
        final float correctionFactor;

        TestAdjustment(long fileNum,
                       long endFileNum,
                       FileSummary estimatedFileSummary,
                       FileSummary trueFileSummary,
                       float estimatedAvgSize,
                       float trueAvgSize,
                       float correctionFactor) {
            this.fileNum = fileNum;
            this.endFileNum = endFileNum;
            this.estimatedFileSummary = estimatedFileSummary;
            this.trueFileSummary = trueFileSummary;
            this.estimatedAvgSize = estimatedAvgSize;
            this.trueAvgSize = trueAvgSize;
            this.correctionFactor = correctionFactor;
        }
    }
}
