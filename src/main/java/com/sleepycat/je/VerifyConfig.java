/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import java.io.PrintStream;

/**
 * Specifies the attributes of a verification operation.
 */
public class VerifyConfig implements Cloneable {

    /*
     * For internal use, to allow null as a valid value for the config
     * parameter.
     */
    public static final VerifyConfig DEFAULT = new VerifyConfig();

    private boolean propagateExceptions = false;
    private boolean aggressive = false;
    private boolean printInfo = false;
    private PrintStream showProgressStream = null;
    private int showProgressInterval = 0;

    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public VerifyConfig() {
    }

    /**
     * Configures {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} to propagate exceptions found during verification.
     *
     * <p>By default this is false and exception information is printed to
     * System.out for notification but does not stop the verification activity,
     * which continues on for as long as possible.</p>
     *
     * @param propagate If set to true, configure {@link
     * com.sleepycat.je.Environment#verify Environment.verify} and {@link
     * com.sleepycat.je.Database#verify Database.verify} to propagate
     * exceptions found during verification.
     *
     * @return this
     */
    public VerifyConfig setPropagateExceptions(boolean propagate) {
        setPropagateExceptionsVoid(propagate);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setPropagateExceptionsVoid(boolean propagate) {
        propagateExceptions = propagate;
    }

    /**
     * Returns true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to propagate exceptions found during
     * verification.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to propagate exceptions found during
     * verification.
     */
    public boolean getPropagateExceptions() {
        return propagateExceptions;
    }

    /**
     * Configures {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} to perform fine granularity consistency checking that
     * includes verifying in memory constructs.
     *
     * <p>This level of checking should only be performed while the database
     * environment is quiescent.</p>
     *
     * <p>By default this is false.</p>
     *
     * @param aggressive If set to true, configure {@link
     * com.sleepycat.je.Environment#verify Environment.verify} and {@link
     * com.sleepycat.je.Database#verify Database.verify} to perform fine
     * granularity consistency checking that includes verifying in memory
     * constructs.
     *
     * @return this
     */
    public VerifyConfig setAggressive(boolean aggressive) {
        setAggressiveVoid(aggressive);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setAggressiveVoid(boolean aggressive) {
        this.aggressive = aggressive;
    }

    /**
     * Returns true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to perform fine granularity consistency
     * checking that includes verifying in memory constructs.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to perform fine granularity consistency
     * checking that includes verifying in memory constructs.
     */
    public boolean getAggressive() {
        return aggressive;
    }

    /**
     * Configures {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} to print basic verification information to System.out.
     *
     * <p>By default this is false.</p>
     *
     * @param printInfo If set to true, configure {@link
     * com.sleepycat.je.Environment#verify Environment.verify} and {@link
     * com.sleepycat.je.Database#verify Database.verify} to print basic
     * verification information to System.out.
     *
     * @return this
     */
    public VerifyConfig setPrintInfo(boolean printInfo) {
        setPrintInfoVoid(printInfo);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setPrintInfoVoid(boolean printInfo) {
        this.printInfo = printInfo;
    }

    /**
     * Returns true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to print basic verification information
     * to System.out.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to print basic verification information
     * to System.out.
     */
    public boolean getPrintInfo() {
        return printInfo;
    }

    /**
     * Configures the verify operation to display progress to the PrintStream
     * argument.  The accumulated statistics will be displayed every N records,
     * where N is the value of showProgressInterval.
     *
     * @return this
     */
    public VerifyConfig setShowProgressStream(PrintStream showProgressStream) {
        setShowProgressStreamVoid(showProgressStream);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setShowProgressStreamVoid(PrintStream showProgressStream) {
        this.showProgressStream = showProgressStream;
    }

    /**
     * Returns the PrintStream on which the progress messages will be displayed
     * during long running verify operations.
     */
    public PrintStream getShowProgressStream() {
        return showProgressStream;
    }

    /**
     * When the verify operation is configured to display progress the
     * showProgressInterval is the number of LNs between each progress report.
     *
     * @return this
     */
    public VerifyConfig setShowProgressInterval(int showProgressInterval) {
        setShowProgressIntervalVoid(showProgressInterval);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setShowProgressIntervalVoid(int showProgressInterval) {
        this.showProgressInterval = showProgressInterval;
    }

    /**
     * Returns the showProgressInterval value, if set.
     */
    public int getShowProgressInterval() {
        return showProgressInterval;
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public VerifyConfig clone() {
        try {
            return (VerifyConfig) super.clone();
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
        StringBuilder sb = new StringBuilder();
        sb.append("propagateExceptions=").append(propagateExceptions);
        return sb.toString();
    }
}
