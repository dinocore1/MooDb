/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import java.io.Closeable;
import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import javax.transaction.xa.Xid;

import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbEnvPool;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.RepConfigProxy;
import com.sleepycat.je.dbi.StartupTracker.Phase;
import com.sleepycat.je.dbi.TriggerManager;
import com.sleepycat.je.dbi.DbTree.TruncateDbResult;
import com.sleepycat.je.txn.HandleLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DatabaseUtil;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * A database environment.  Environments include support for some or all of
 * caching, locking, logging and transactions.
 *
 * <p>To open an existing environment with default attributes the application
 * may use a default environment configuration object or null:
 * <blockquote>
 *     <pre>
 *      // Open an environment handle with default attributes.
 *     Environment env = new Environment(home, new EnvironmentConfig());
 *     </pre>
 * </blockquote>
 * or
 * <blockquote><pre>
 *     Environment env = new Environment(home, null);
 * </pre></blockquote>
 * <p>Note that many Environment objects may access a single environment.</p>
 * <p>To create an environment or customize attributes, the application should
 * customize the configuration class. For example:</p>
 * <blockquote><pre>
 *     EnvironmentConfig envConfig = new EnvironmentConfig();
 *     envConfig.setTransactional(true);
 *     envConfig.setAllowCreate(true);
 *     envConfig.setCacheSize(1000000);
 *     Environment newlyCreatedEnv = new Environment(home, envConfig);
 * </pre></blockquote>
 *
 * <p>Note that environment configuration parameters can also be set through
 * the &lt;environment home&gt;/je.properties file. This file takes precedence
 * over any programmatically specified configuration parameters so that
 * configuration changes can be made without recompiling. Environment
 * configuration follows this order of precedence:</p>
 *
 * <ol>
 * <li>Configuration parameters specified in
 * &lt;environment home&gt;/je.properties take first precedence.
 * <li> Configuration parameters set in the EnvironmentConfig object used at
 * Environment construction are next.
 * <li>Any configuration parameters not set by the application are set to
 * system defaults, described along with the parameter name String constants
 * in the EnvironmentConfig class.
 * </ol>
 *
 * <p>An <em>environment handle</em> is an Environment instance.  More than one
 * Environment instance may be created for the same physical directory, which
 * is the same as saying that more than one Environment handle may be open at
 * one time for a given environment.</p>
 *
 * The Environment handle should not be closed while any other handle remains
 * open that is using it as a reference (for example, {@link
 * com.sleepycat.je.Database Database} or {@link com.sleepycat.je.Transaction
 * Transaction}.  Once {@link com.sleepycat.je.Environment#close
 * Environment.close} is called, this object may not be accessed again.
 */
public class Environment implements Closeable {

    /**
     * @hidden
     * envImpl is a reference to the shared underlying environment.
     */
    protected EnvironmentImpl envImpl;
    private TransactionConfig defaultTxnConfig;
    private EnvironmentMutableConfig handleConfig;
    private final EnvironmentConfig appliedFinalConfig;

    private final Set<Database> referringDbs;
    private final Set<Transaction> referringDbTxns;

    /**
     * @hidden
     * The name of the cleaner daemon thread.  This constant is passed to an
     * ExceptionEvent's threadName argument when an exception is thrown in the
     * cleaner daemon thread.
     */
    public static final String CLEANER_NAME = "Cleaner";

    /**
     * @hidden
     * The name of the IN Compressor daemon thread.  This constant is passed to
     * an ExceptionEvent's threadName argument when an exception is thrown in
     * the IN Compressor daemon thread.
     */
    public static final String INCOMP_NAME = "INCompressor";

    /**
     * @hidden
     * The name of the Checkpointer daemon thread.  This constant is passed to
     * an ExceptionEvent's threadName argument when an exception is thrown in
     * the Checkpointer daemon thread.
     */
    public static final String CHECKPOINTER_NAME = "Checkpointer";

    /**
     * Creates a database environment handle.
     *
     * @param envHome The database environment's home directory.
     *
     * @param configuration The database environment attributes.  If null,
     * default attributes are used.
     *
     * @throws EnvironmentNotFoundException if the environment does not exist
     * (does not contain at least one log file) and the {@code
     * EnvironmentConfig AllowCreate} parameter is false.
     *
     * @throws EnvironmentLockedException when an environment cannot be opened
     * for write access because another process has the same environment open
     * for write access.  <strong>Warning:</strong> This exception should be
     * handled when an environment is opened by more than one process.
     *
     * @throws VersionMismatchException when the existing log is not compatible
     * with the version of JE that is running.  This occurs when a later
     * version of JE was used to create the log.  <strong>Warning:</strong>
     * This exception should be handled when more than one version of JE may be
     * used to access an environment.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this environment was previously
     * opened for replication and is not being opened read-only.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code EnvironmentConfig} parameter.
     */
    public Environment(File envHome, EnvironmentConfig configuration)
        throws EnvironmentNotFoundException,
               EnvironmentLockedException,
               VersionMismatchException,
               DatabaseException,
               IllegalArgumentException {

        this(envHome, configuration, true /*openIfNeeded*/,
             null /*repConfigProxy*/, null /*envImplParam*/);
    }

    /**
     * @hidden
     * Replication support.
     */
    protected Environment(File envHome,
                          EnvironmentConfig configuration,
                          RepConfigProxy repConfigProxy,
                          EnvironmentImpl envImplParam) {
        this(envHome, configuration, true /*openIfNeeded*/, repConfigProxy,
             envImplParam);
    }

    /**
     * Gets an Environment for an existing EnvironmentImpl. Used by utilities
     * such as the JMX MBean which don't want to open the environment or be
     * reference counted. The calling application must take care not to retain
     */
    Environment(File envHome) {
        this(envHome, null /*configuration*/, false /*openIfNeeded*/,
             null /*repConfigProxy*/, null /*envImplParam*/);
    }

    /**
     * @hidden
     * Internal common constructor.
     *
     * @param envImpl is non-null only when used by EnvironmentIml to create an
     * InternalEnvironment.
     */
    protected Environment(File envHome,
                          EnvironmentConfig envConfig,
                          boolean openIfNeeded,
                          RepConfigProxy repConfigProxy,
                          EnvironmentImpl envImplParam) {

        /* If openIfNeeded is false, then envConfig must be null. */
        assert openIfNeeded || envConfig == null || envImplParam != null;

        envImpl = null;
        referringDbs = Collections.synchronizedSet(new HashSet<Database>());
        referringDbTxns =
            Collections.synchronizedSet(new HashSet<Transaction>());

        DatabaseUtil.checkForNullParam(envHome, "envHome");

        appliedFinalConfig =
            setupHandleConfig(envHome, envConfig, repConfigProxy);

        if (envImplParam != null) {
            /* We're creating an InternalEnvironment in EnvironmentImpl. */
            envImpl = envImplParam;
        } else {
            /* Open a new or existing environment in the shared pool. */
            envImpl = makeEnvironmentImpl(envHome,
                                          envConfig,
                                          openIfNeeded,
                                          repConfigProxy);
        }
    }

    /**
     * @hidden
     * MakeEnvironmentImpl is called both by the Environment constructor and
     * by the ReplicatedEnvironment constructor when recreating the environment
     * for a hard recovery.
     */
    protected EnvironmentImpl makeEnvironmentImpl
        (File envHome,
         EnvironmentConfig envConfig,
         boolean openIfNeeded,
         RepConfigProxy repConfigProxy) {

        envImpl = DbEnvPool.getInstance().getEnvironment
            (envHome,
             appliedFinalConfig,
             envConfig != null /*checkImmutableParams*/,
             openIfNeeded,
             setupRepConfig(envHome, repConfigProxy, envConfig));

        if (envImpl != null) {
            envImpl.registerMBean(this);
        }

        return envImpl;
    }

    /**
     * Validate the parameters specified in the environment config.  Applies
     * the configurations specified in the je.properties file to override any
     * programmatically set configurations.  Create a copy to save in this
     * handle. The main reason to return a config instead of using the
     * handleConfig field is to return an EnvironmentConfig instead of a
     * EnvironmentMutableConfig.
     */
    private EnvironmentConfig setupHandleConfig(File envHome,
                                                EnvironmentConfig envConfig,
                                                RepConfigProxy repConfig)
        throws IllegalArgumentException {

        /* If the user specified a null object, use the default */
        EnvironmentConfig baseConfig = (envConfig == null) ?
            EnvironmentConfig.DEFAULT : envConfig;

        /* Make a copy, apply je.properties, and init the handle config. */
        EnvironmentConfig useConfig = baseConfig.clone();

        /* Apply the je.properties file. */
        if (useConfig.getLoadPropertyFile()) {
            DbConfigManager.applyFileConfig(envHome,
                                            DbInternal.getProps(useConfig),
                                            false);       // forReplication
        }
        copyToHandleConfig(useConfig, useConfig, repConfig);
        return useConfig;
    }

    /**
     * @hidden
     * Obtain a validated replication configuration. In a non-HA environment,
     * return null.
     */
    protected RepConfigProxy
        setupRepConfig(final File envHome,
                       final RepConfigProxy repConfigProxy,
                       final EnvironmentConfig envConfig) {

        return null;
    }

    /**
     * The Environment.close method closes the Berkeley DB environment.
     *
     * <p>When the last environment handle is closed, allocated resources are
     * freed, and daemon threads are stopped, even if they are performing work.
     * For example, if the cleaner is still cleaning the log, it will be
     * stopped at the next reasonable opportunity and perform no more cleaning
     * operations.</p>
     *
     * <p>The Environment handle should not be closed while any other handle
     * that refers to it is not yet closed; for example, database environment
     * handles must not be closed while database handles remain open, or
     * transactions in the environment have not yet committed or aborted.
     * Specifically, this includes {@link com.sleepycat.je.Database Database},
     * {@link com.sleepycat.je.Cursor Cursor} and {@link
     * com.sleepycat.je.Transaction Transaction} handles.</p>
     *
     * <p>If this handle has already been closed, this method does nothing and
     * returns without throwing an exception.</p>
     *
     * <p>In multithreaded applications, only a single thread should call
     * Environment.close.</p>
     *
     * <p>The environment handle may not be used again after this method has
     * been called, regardless of the method's success or failure, with one
     * exception:  the {@code close} method itself may be called any number of
     * times.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if any open databases or transactions
     * refer to this handle.
     */
    public synchronized void close()
        throws DatabaseException {

        if (envImpl == null) {
            return;
        }

        if (!envImpl.isValid()) {

            /*
             * We're trying to close on an environment that has seen a fatal
             * exception. Try to do the minimum, such as closing file
             * descriptors, to support re-opening the environment in the same
             * JVM.
             */
            try {
                envImpl.closeAfterInvalid();
            } finally {
                envImpl = null;
            }
            return;
        }

        final StringBuilder errors = new StringBuilder();
        try {
            checkForCloseErrors(errors);

            try {
                envImpl.close();
            } catch (RuntimeException e) {
                if (!envImpl.isValid()) {
                    /* Propagate if env is invalidated. */
                    throw e;
                }
                errors.append
                  ("\nWhile closing Environment encountered exception: ");
                errors.append(e).append("\n");
            }

            if (errors.length() > 0) {
                throw new IllegalStateException(errors.toString());
            }
        } finally {
            envImpl = null;
        }
    }

    /**
     * Close an InternalEnvironment handle.  We do not call
     * EnvironmentImpl.close here, since an InternalEnvironment is not
     * registered like a non-internal handle.  However, we must call
     * checkForCloseErrors to auto-close internal databases, as well as check
     * for errors.
     */
    synchronized void closeInternalHandle() {
        final StringBuilder errors = new StringBuilder();
        checkForCloseErrors(errors);
        if (errors.length() > 0) {
            throw new IllegalStateException(errors.toString());
        }
    }

    private void checkForCloseErrors(StringBuilder errors) {

        checkOpenDbs(errors);

        checkOpenTxns(errors);

        if (!isInternalHandle()) {

            /*
             * Only check for open XA transactions against user created
             * environment handles.
             */
            checkOpenXATransactions(errors);
        }
    }

    /**
     * Appends error messages to the errors argument if there are
     * open XA transactions associated with the underlying EnvironmentImpl.
     */
    private void checkOpenXATransactions(final StringBuilder errors) {
        Xid[] openXids = envImpl.getTxnManager().XARecover();
        if (openXids != null && openXids.length > 0) {
            errors.append("There ");
            int nXATxns = openXids.length;
            if (nXATxns == 1) {
                errors.append("is 1 existing XA transaction opened");
                errors.append(" in the Environment.\n");
                errors.append("It");
            } else {
                errors.append("are ");
                errors.append(nXATxns);
                errors.append(" existing transactions opened in");
                errors.append(" the Environment.\n");
                errors.append("They");
            }
            errors.append(" will be left open ...\n");
        }
    }

    /**
     * Appends error messages to the errors argument if there are open
     * transactions associated with the environment.
     */
    private void checkOpenTxns(final StringBuilder errors) {
        int nTxns = (referringDbTxns == null) ? 0 : referringDbTxns.size();
        if (nTxns == 0) {
            return;
        }

        /*
         * Copy the referringDbTxns Set because txn.abort() modifies this Set,
         * potentially causing a ConcurrentModificationException.  Must do the
         * copy while synchronized. [#21279]
         */
        final Iterator<Transaction> iter;
        synchronized (referringDbTxns) {
            iter = new HashSet<Transaction>(referringDbTxns).iterator();
        }
        errors.append("There ");
        if (nTxns == 1) {
            errors.append("is 1 existing transaction opened");
            errors.append(" against the Environment.\n");
        } else {
            errors.append("are ");
            errors.append(nTxns);
            errors.append(" existing transactions opened against");
            errors.append(" the Environment.\n");
        }
        errors.append("Aborting open transactions ...\n");

        while (iter.hasNext()) {
            Transaction txn = iter.next();
            try {
                errors.append("aborting " + txn);
                txn.abort();
            } catch (RuntimeException e) {
                if (!envImpl.isValid()) {
                    /* Propagate if env is invalidated. */
                    throw e;
                }
                errors.append("\nWhile aborting transaction ");
                errors.append(txn.getId());
                errors.append(" encountered exception: ");
                errors.append(e).append("\n");
            }
        }
    }

    /**
     * Appends error messages to the errors argument if there are open database
     * handles associated with the environment.
     */
    private void checkOpenDbs(final StringBuilder errors) {

        if (referringDbs.isEmpty()) {
            return;
        }

        /*
         * Copy the referringDbs Set because db.close() modifies this Set,
         * potentially causing a ConcurrentModificationException.  Must do the
         * copy while synchronized. [#21279]
         */
        final Iterator<Database> iter;
        synchronized (referringDbs) {
            iter = new HashSet<Database>(referringDbs).iterator();
        }
        int nOpenUserDbs = 0;
        while (iter.hasNext()) {
            String dbName = "";
            try {
                Database db = iter.next();

                /*
                 * Save the db name before we attempt the close, it's
                 * unavailable after the close.
                 */
                dbName = db.getDebugName();

                if (!db.getDatabaseImpl().isInternalDb()) {
                    nOpenUserDbs += 1;
                    errors.append("Unclosed Database: ");
                    errors.append(dbName).append("\n");
                }
                db.close();
            } catch (RuntimeException e) {
                if (!envImpl.isValid()) {
                    /* Propagate if env is invalidated. */
                    throw e;
                }
                errors.append("\nWhile closing Database ");
                errors.append(dbName);
                errors.append(" encountered exception: ");
                errors.append(e).append("\n");
            }
        }

        if (nOpenUserDbs > 0) {
            errors.append("Databases left open: ");
            errors.append(nOpenUserDbs).append("\n");
        }
    }

    /**
     * Opens, and optionally creates, a <code>Database</code>.
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param databaseName The name of the database.
     *
     * @param dbConfig The database attributes.  If null, default attributes
     * are used.
     *
     * @return Database handle.
     *
     * @throws DatabaseExistsException if the database already exists and the
     * {@code DatabaseConfig ExclusiveCreate} parameter is true.
     *
     * @throws DatabaseNotFoundException if the database does not exist and the
     * {@code DatabaseConfig AllowCreate} parameter is false.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs. If the database does not exist and the {@link
     * DatabaseConfig#setAllowCreate AllowCreate} parameter is true, then one
     * of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> may also occur.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code DatabaseConfig} property.
     *
     * @throws IllegalStateException if DatabaseConfig properties are changed
     * and there are other open handles for this database.
     */
    public synchronized Database openDatabase(Transaction txn,
                                              String databaseName,
                                              DatabaseConfig dbConfig)
        throws DatabaseNotFoundException,
               DatabaseExistsException,
               IllegalArgumentException,
               IllegalStateException {

        checkHandleIsValid();
        checkEnv();

        try {
            if (dbConfig == null) {
                dbConfig = DatabaseConfig.DEFAULT;
            }

            Database db = new Database(this);
            setupDatabase(txn, db, databaseName, dbConfig,
                          false /*isInternalDb*/);
            return db;
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Opens an internal database for internal JE use.
     *  - permits opening DBs with reserved (internal) names.
     *  - permits opening a transactional DB in a non-transactional env.
     *
     * DbConfig should not be null. Note that the dbConfig argument determines
     * whether the database that is created is to be replicated or is
     * standalone. The current default is for dbConfig to be replicated, so use
     * DatabaseConfig.setReplicated(false) to create a local internal database.
     */
    synchronized Database openInternalDatabase(Transaction txn,
                                               String databaseName,
                                               DatabaseConfig dbConfig)
        throws DatabaseNotFoundException, DatabaseExistsException {

        assert DbTree.isReservedDbName(databaseName) : databaseName;
        Database db = new Database(this);
        setupDatabase(txn, db, databaseName, dbConfig, true /*isInternalDb*/);
        return db;
    }

    /**
     * Opens and optionally creates a <code>SecondaryDatabase</code>.
     *
     * <p>Note that the associations between primary and secondary databases
     * are not stored persistently.  Whenever a primary database is opened for
     * write access by the application, the appropriate associated secondary
     * databases should also be opened by the application.  This is necessary
     * to ensure data integrity when changes are made to the primary
     * database.</p>
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param databaseName The name of the database.
     *
     * @param primaryDatabase the primary database with which the secondary
     * database will be associated.  The primary database must not be
     * configured for duplicates.
     *
     * @param dbConfig The secondary database attributes.  If null, default
     * attributes are used.
     *
     * @return Database handle.
     *
     * @throws DatabaseExistsException if the database already exists and the
     * {@code DatabaseConfig ExclusiveCreate} parameter is true.
     *
     * @throws DatabaseNotFoundException if the database does not exist and the
     * {@code DatabaseConfig AllowCreate} parameter is false.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs. If the database does not exist and the {@link
     * DatabaseConfig#setAllowCreate AllowCreate} parameter is true, then one
     * of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> may also occur.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code SecondaryConfig} property.
     *
     * @throws IllegalStateException if DatabaseConfig properties are changed
     * and there are other open handles for this database.
     */
    public synchronized
        SecondaryDatabase openSecondaryDatabase(Transaction txn,
                                                String databaseName,
                                                Database primaryDatabase,
                                                SecondaryConfig dbConfig)
        throws DatabaseNotFoundException,
               DatabaseExistsException,
               DatabaseException,
               IllegalArgumentException,
               IllegalStateException {

        checkHandleIsValid();
        checkEnv();
        try {
            if (dbConfig == null) {
                dbConfig = SecondaryConfig.DEFAULT;
            }
            SecondaryDatabase db =
                new SecondaryDatabase(this, dbConfig, primaryDatabase);

            setupDatabase(txn, db, databaseName, dbConfig,
                          false /*isInternalDb*/);
            return db;
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * The meat of open database processing.
     *
     * @param txn may be null
     * @param newDb is the Database handle which houses this database
     *
     * @throws IllegalArgumentException via openDatabase and
     * openSecondaryDatabase
     *
     * @see HandleLocker
     */
    private void setupDatabase(Transaction txn,
                               Database newDb,
                               String databaseName,
                               DatabaseConfig dbConfig,
                               boolean isInternalDb)
        throws DatabaseNotFoundException, DatabaseExistsException {

        checkEnv();
        DatabaseUtil.checkForNullParam(databaseName, "databaseName");

        LoggerUtils.envLogMsg(Level.FINEST, envImpl,
                              "Environment.open: " + " name=" + databaseName +
                              " dbConfig=" + dbConfig);

        final boolean autoTxnIsReplicated =
            DbInternal.getReplicated(dbConfig) && envImpl.isReplicated();

        /*
         * Check that the open configuration is valid and doesn't conflict with
         * the envImpl configuration.
         */
        dbConfig.validateOnDbOpen(databaseName, autoTxnIsReplicated);
        validateDbConfigAgainstEnv(dbConfig, databaseName, isInternalDb);

        /* Perform eviction before each operation that allocates memory. */
        envImpl.criticalEviction(false /*backgroundIO*/);

        DatabaseImpl database = null;
        boolean operationOk = false;
        HandleLocker handleLocker = null;
        final Locker locker = LockerFactory.getWritableLocker
            (this, txn, isInternalDb, dbConfig.getTransactional(),
             autoTxnIsReplicated, null);
        try {

            /*
             * Create the handle locker and lock the NameLN of an existing
             * database.  A read lock on the NameLN is acquired for both locker
             * and handleLocker.  Note: getDb may return a deleted database.
             */
            handleLocker = newDb.initHandleLocker(envImpl, locker);
            database = envImpl.getDbTree().getDb(locker, databaseName,
                                                 handleLocker);

            boolean dbCreated = false;
            final boolean databaseExists =
                (database != null) && !database.isDeleted();

            if (databaseExists) {
                if (dbConfig.getAllowCreate() &&
                    dbConfig.getExclusiveCreate()) {
                    throw new DatabaseExistsException
                        ("Database " + databaseName + " already exists");
                }

                newDb.initExisting(this, locker, database, databaseName,
                                   dbConfig);
            } else {
                /* Release deleted DB. [#13415] */
                envImpl.getDbTree().releaseDb(database);
                database = null;

                if (!isInternalDb &&
                    DbTree.isReservedDbName(databaseName)) {
                    throw new IllegalArgumentException
                        (databaseName + " is a reserved database name.");
                }

                if (!dbConfig.getAllowCreate()) {
                    throw new DatabaseNotFoundException("Database " +
                                                        databaseName +
                                                        " not found.");
                }

                /*
                 * Init a new DB. This calls DbTree.createDb and the new
                 * database is returned.  A write lock on the NameLN is
                 * acquired by locker and a read lock by the handleLocker.
                 */
                database = newDb.initNew(this, locker, databaseName, dbConfig);
                dbCreated = true;
            }

            /*
             * The open is successful.  We add the opened database handle to
             * this environment to track open handles in general, and to the
             * locker so that it can be invalidated by a user txn abort.
             */
            operationOk = true;
            addReferringHandle(newDb);
            locker.addOpenedDatabase(newDb);

            /* Run triggers before any subsequent auto commits. */
            final boolean firstWriteHandle =
                newDb.isWritable() &&
                (newDb.getDatabaseImpl().noteWriteHandleOpen() == 1);

            if (dbCreated || firstWriteHandle) {
                TriggerManager.runOpenTriggers(locker, newDb, dbCreated);
            }
        } finally {

            /*
             * If the open fails, decrement the DB usage count and release
             * handle locks.  In other cases this is done by Database.close()
             * or invalidate(), the latter in the case of a user txn abort.
             */
            if (!operationOk) {
                envImpl.getDbTree().releaseDb(database);
                if (handleLocker != null) {
                    handleLocker.operationEnd(false);
                }
            }

            /*
             * Tell the locker that this operation is over. Some types of
             * lockers (BasicLocker and auto Txn) will actually finish.
             */
            locker.operationEnd(operationOk);
        }
    }

    /**
     * @throws IllegalArgumentException via openDatabase and
     * openSecondaryDatabase
     */
    private void validateDbConfigAgainstEnv(DatabaseConfig dbConfig,
                                            String databaseName,
                                            boolean isInternalDb)
        throws IllegalArgumentException {

        /*
         * R/W database handles on a replicated database must be transactional,
         * for now. In the future we may support non-transactional database
         * handles.
         */
        if (envImpl.isReplicated() &&
            dbConfig.getReplicated() &&
            !dbConfig.getReadOnly()) {
            if (!dbConfig.getTransactional()) {
                throw new IllegalArgumentException
                ("Read/Write Database instances for replicated " +
                 "database " + databaseName + " must be transactional.");
            }
        }

        /* Check operation's transactional status against the Environment */
        if (!isInternalDb &&
            dbConfig.getTransactional() &&
            !(envImpl.isTransactional())) {
            throw new IllegalArgumentException
                ("Attempted to open Database " + databaseName +
                 " transactionally, but parent Environment is" +
                 " not transactional");
        }

        /* Check read/write status */
        if (envImpl.isReadOnly() && (!dbConfig.getReadOnly())) {
            throw new IllegalArgumentException
                ("Attempted to open Database " + databaseName +
                 " as writable but parent Environment is read only ");
        }
    }

    /**
     * Removes a database from the environment, discarding all records in the
     * database and removing the database name itself.
     *
     * <p>Compared to deleting all the records in a database individually,
     * {@code removeDatabase} is a very efficient operation.  Some internal
     * housekeeping information is updated, but the database records are not
     * read or written, and very little I/O is needed.</p>
     *
     * <p>When called on a database configured with secondary indices, the
     * application is responsible for also removing all associated secondary
     * indices.  To guarantee integrity, a primary database and all of its
     * secondary databases should be removed atomically using a single
     * transaction.</p>
     *
     * <p>Applications should not remove a database with open {@link Database
     * Database} handles.  If the database is open with the same transaction as
     * passed in the {@code txn} parameter, {@link IllegalStateException} is
     * thrown by this method.  If the database is open using a different
     * transaction, this method will block until all database handles are
     * closed, or until the conflict is resolved by throwing {@link
     * LockConflictException}.</p>
     *
     * @param txn For a transactional environment, an explicit transaction
     * may be specified or null may be specified to use auto-commit.  For a
     * non-transactional environment, null must be specified.
     *
     * @param databaseName The database to be removed.
     *
     * @throws DatabaseNotFoundException if the database does not exist.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is a read-only
     * environment.
     *
     * @throws IllegalStateException if the database is currently open using
     * the transaction passed in the {@code txn} parameter, or if this handle
     * or the underlying environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public void removeDatabase(Transaction txn, String databaseName)
        throws DatabaseNotFoundException {

        checkHandleIsValid();
        DatabaseUtil.checkForNullParam(databaseName, "databaseName");
        checkEnv();
        checkWritable();

        final boolean autoTxnIsReplicated = envImpl.isReplicated();
        Locker locker = null;
        DatabaseImpl dbImpl = null;
        try {

            /*
             * Note: use env level isTransactional as proxy for the db
             * isTransactional.
             */
            locker = LockerFactory.getWritableLocker
                (this,
                 txn,
                 false /*isInternalDb*/,
                 envImpl.isTransactional(),
                 autoTxnIsReplicated,
                 null);
            dbImpl = envImpl.getDbTree().dbRemove(locker,
                                         databaseName,
                                         null /*checkId*/);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        } finally {
            if (dbImpl != null) {
                TriggerManager.runRemoveTriggers(locker, dbImpl);
            }
            if (locker != null) {
                locker.operationEnd(dbImpl != null);
            }
        }
    }

    /**
     * Renames a database, without removing the records it contains.
     *
     * <p>Applications should not rename a database with open {@link Database
     * Database} handles.  If the database is open with the same transaction as
     * passed in the {@code txn} parameter, {@link IllegalStateException} is
     * thrown by this method.  If the database is open using a different
     * transaction, this method will block until all database handles are
     * closed, or until the conflict is resolved by throwing {@link
     * LockConflictException}.</p>
     *
     * @param txn For a transactional environment, an explicit transaction
     * may be specified or null may be specified to use auto-commit.  For a
     * non-transactional environment, null must be specified.
     *
     * @param databaseName The new name of the database.
     *
     * @throws DatabaseNotFoundException if the database does not exist.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is a read-only
     * environment.
     *
     * @throws IllegalStateException if the database is currently open using
     * the transaction passed in the {@code txn} parameter, or if this handle
     * or the underlying environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public void renameDatabase(Transaction txn,
                               String databaseName,
                               String newName)
        throws DatabaseNotFoundException {

        DatabaseUtil.checkForNullParam(databaseName, "databaseName");
        DatabaseUtil.checkForNullParam(newName, "newName");

        checkHandleIsValid();
        checkEnv();
        checkWritable();

        Locker locker = null;
        DatabaseImpl dbImpl = null;
        try {

            /*
             * Note: use env level isTransactional as proxy for the db
             * isTransactional.
             */
            locker = LockerFactory.getWritableLocker
                (this, txn,
                 false /*isInternalDb*/,
                 envImpl.isTransactional(),
                 envImpl.isReplicated(),  // autoTxnIsReplicated
                 null);
            dbImpl = envImpl.getDbTree().
                dbRename(locker, databaseName, newName);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        } finally {
            if (dbImpl != null) {
                TriggerManager.runRenameTriggers(locker, dbImpl, newName);
            }
            if (locker != null) {
                locker.operationEnd(dbImpl != null);
            }
        }
    }

    /**
     * Empties the database, discarding all the records it contains, without
     * removing the database name.
     *
     * <p>Compared to deleting all the records in a database individually,
     * {@code truncateDatabase} is a very efficient operation.  Some internal
     * housekeeping information is updated, but the database records are not
     * read or written, and very little I/O is needed.</p>
     *
     * <p>When called on a database configured with secondary indices, the
     * application is responsible for also truncating all associated secondary
     * indices.  To guarantee integrity, a primary database and all of its
     * secondary databases should be truncated atomically using a single
     * transaction.</p>
     *
     * <p>Applications should not truncate a database with open {@link Database
     * Database} handles.  If the database is open with the same transaction as
     * passed in the {@code txn} parameter, {@link IllegalStateException} is
     * thrown by this method.  If the database is open using a different
     * transaction, this method will block until all database handles are
     * closed, or until the conflict is resolved by throwing {@link
     * LockConflictException}.</p>
     *
     * @param txn For a transactional environment, an explicit transaction may
     * be specified or null may be specified to use auto-commit.  For a
     * non-transactional environment, null must be specified.
     *
     * @param databaseName The database to be truncated.
     *
     * @param returnCount If true, count and return the number of records
     * discarded.
     *
     * @return The number of records discarded, or -1 if returnCount is false.
     *
     * @throws DatabaseNotFoundException if the database does not exist.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is a read-only
     * environment.
     *
     * @throws IllegalStateException if the database is currently open using
     * the transaction passed in the {@code txn} parameter, or if this handle
     * or the underlying environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public long truncateDatabase(Transaction txn,
                                 String databaseName,
                                 boolean returnCount)
        throws DatabaseNotFoundException {

        checkHandleIsValid();
        DatabaseUtil.checkForNullParam(databaseName, "databaseName");
        checkEnv();
        checkWritable();

        final boolean autoTxnIsReplicated = envImpl.isReplicated();
        Locker locker = null;
        TruncateDbResult result = null;
        try {

            /*
             * Note: use env level isTransactional as proxy for the db
             * isTransactional.
             */
            locker = LockerFactory.getWritableLocker
                (this, txn,
                 false /*isInternalDb*/,
                 envImpl.isTransactional(),
                 autoTxnIsReplicated,
                 null);

            result = envImpl.getDbTree().truncate(locker,
                                                 databaseName,
                                                 returnCount);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        } finally {
            if (result != null) {
                TriggerManager.runTruncateTriggers(locker, result.newDb);
            }
            if (locker != null) {
                locker.operationEnd(result != null);
            }
        }

        if (result == null) {
            /* Added to suppress compiler warnings. */
            throw EnvironmentFailureException.unexpectedState(envImpl,
                                                              "Cannot happen");
        }

        return result.recordCount;
    }

    /**
     * For unit testing.  Returns the current memory usage in bytes for all
     * btrees in the envImpl.
     */
    long getMemoryUsage()
        throws DatabaseException {

        checkHandleIsValid();
        checkEnv();

        return envImpl.getMemoryBudget().getCacheMemoryUsage();
    }

    /**
     * Returns the database environment's home directory.
     *
     * @return The database environment's home directory.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public File getHome()
        throws DatabaseException {

        checkHandleIsValid();

        return envImpl.getEnvironmentHome();
    }

    /*
     * Transaction management
     */

    /**
     * Returns the default txn config for this environment handle.
     */
    TransactionConfig getDefaultTxnConfig() {
        return defaultTxnConfig;
    }

    /**
     * Copies the handle properties out of the config properties, and
     * initializes the default transaction config.
     */
    private void copyToHandleConfig(EnvironmentMutableConfig useConfig,
                                    EnvironmentConfig initStaticConfig,
                                    RepConfigProxy    initRepConfig) {

        /*
         * Create the new objects, initialize them, then change the instance
         * fields.  This avoids synchronization issues.
         */
        EnvironmentMutableConfig newHandleConfig =
            new EnvironmentMutableConfig();
        useConfig.copyHandlePropsTo(newHandleConfig);
        this.handleConfig = newHandleConfig;

        TransactionConfig newTxnConfig =
            TransactionConfig.DEFAULT.clone();
        newTxnConfig.setNoSync(handleConfig.getTxnNoSync());
        newTxnConfig.setWriteNoSync(handleConfig.getTxnWriteNoSync());
        newTxnConfig.setDurability(handleConfig.getDurability());

        if (initStaticConfig != null) {
            newTxnConfig.setSerializableIsolation
                (initStaticConfig.getTxnSerializableIsolation());
            newTxnConfig.setReadCommitted
                (initStaticConfig.getTxnReadCommitted());
        } else {
            newTxnConfig.setSerializableIsolation
                (defaultTxnConfig.getSerializableIsolation());
            newTxnConfig.setReadCommitted
                (defaultTxnConfig.getReadCommitted());
            newTxnConfig.setConsistencyPolicy
                (defaultTxnConfig.getConsistencyPolicy());
        }
        if (initRepConfig != null) {
            newTxnConfig.setConsistencyPolicy
                (initRepConfig.getConsistencyPolicy());
        }
        this.defaultTxnConfig = newTxnConfig;
    }

    /**
     * Creates a new transaction in the database environment.
     *
     * <p>Transaction handles are free-threaded; transactions handles may be
     * used concurrently by multiple threads.</p>
     *
     * <p>Cursors may not span transactions; that is, each cursor must be
     * opened and closed within a single transaction. The parent parameter is a
     * placeholder for nested transactions, and must currently be null.</p>
     *
     * @param txnConfig The transaction attributes.  If null, default
     * attributes are used.
     *
     * @return The newly created transaction's handle.
     *
     * @throws com.sleepycat.je.rep.InsufficientReplicasException if the Master
     * in a replicated environment could not contact a quorum of replicas as
     * determined by the {@link ReplicaAckPolicy}.
     *
     * @throws com.sleepycat.je.rep.ReplicaConsistencyException if a replica
     * in a replicated environment cannot become consistent within the timeout
     * period.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is not a transactional
     * environment.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code TransactionConfig} parameter.
     */
    public Transaction beginTransaction(Transaction parent,
                                        TransactionConfig txnConfig)
        throws DatabaseException,
               IllegalArgumentException {

        try {
            return beginTransactionInternal(parent, txnConfig,
                                            false /*isInternalTxn*/);
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Like beginTransaction, but does not require that the Environment is
     * transactional.
     */
    Transaction beginInternalTransaction(TransactionConfig txnConfig) {
        return beginTransactionInternal(null /*parent*/, txnConfig,
                                        true /*isInternalTxn*/);
    }

    /**
     * @throws IllegalArgumentException via beginTransaction.
     * @throws UnsupportedOperationException via beginTransaction.
     */
    private Transaction beginTransactionInternal(Transaction parent,
                                                 TransactionConfig txnConfig,
                                                 boolean isInternalTxn )
        throws DatabaseException {

        checkHandleIsValid();
        checkEnv();

        if (parent != null) {
            throw new IllegalArgumentException
                ("Parent txn is non-null. " +
                 "Nested transactions are not supported.");
        }

        if (!isInternalTxn && !envImpl.isTransactional()) {
            throw new UnsupportedOperationException
                ("Transactions can not be used in a non-transactional " +
                 "environment");
        }

        checkTxnConfig(txnConfig);

        /*
         * Apply txn config defaults.  We don't need to clone unless we have to
         * apply the env default, since we don't hold onto a txn config
         * reference.
         */
        TransactionConfig useConfig = null;
        if (txnConfig == null) {
            useConfig = defaultTxnConfig;
        } else {
            if (defaultTxnConfig.getNoSync() ||
                defaultTxnConfig.getWriteNoSync()) {

                /*
                 * The environment sync settings have been set, check if any
                 * were set in the user's txn config. If none were set in the
                 * user's config, apply the environment defaults
                 */
                if (!txnConfig.getNoSync() &&
                    !txnConfig.getSync() &&
                    !txnConfig.getWriteNoSync()) {
                    useConfig = txnConfig.clone();
                    if (defaultTxnConfig.getWriteNoSync()) {
                        useConfig.setWriteNoSync(true);
                    } else {
                        useConfig.setNoSync(true);
                    }
                }
            }

            if ((defaultTxnConfig.getDurability() != null) &&
                 (txnConfig.getDurability() == null)) {

                /*
                 * Inherit transaction durability from the environment in the
                 * absence of an explicit transaction config durability.
                 */
                if (useConfig == null) {
                    useConfig = txnConfig.clone();
                }
                useConfig.setDurability(defaultTxnConfig.getDurability());
            }

            if ((defaultTxnConfig.getConsistencyPolicy() != null) &&
                (txnConfig.getConsistencyPolicy() == null)) {
                   if (useConfig == null) {
                       useConfig = txnConfig.clone();
                   }
                   useConfig.setConsistencyPolicy
                       (defaultTxnConfig.getConsistencyPolicy());
            }

            /* Apply isolation level default. */
            if (!txnConfig.getSerializableIsolation() &&
                !txnConfig.getReadCommitted() &&
                !txnConfig.getReadUncommitted()) {
                if (defaultTxnConfig.getSerializableIsolation()) {
                    if (useConfig == null) {
                        useConfig = txnConfig.clone();
                    }
                    useConfig.setSerializableIsolation(true);
                } else if (defaultTxnConfig.getReadCommitted()) {
                    if (useConfig == null) {
                        useConfig = txnConfig.clone();
                    }
                    useConfig.setReadCommitted(true);
                }
            }

            /* No environment level defaults applied. */
            if (useConfig == null) {
                useConfig = txnConfig;
            }
        }
        Txn internalTxn = envImpl.txnBegin(parent, useConfig);
        Transaction txn = new Transaction(this, internalTxn);
        addReferringHandle(txn);
        return txn;
    }

    /**
     * Checks the txnConfig object to ensure that its correctly configured and
     * is compatible with the configuration of the Environment.
     *
     * @param txnConfig the configuration being checked.
     *
     * @throws IllegalArgumentException via beginTransaction
     */
    private void checkTxnConfig(TransactionConfig txnConfig)
        throws IllegalArgumentException {

        if (txnConfig == null) {
            return;
        }
        if ((txnConfig.getSerializableIsolation() &&
             txnConfig.getReadUncommitted()) ||
            (txnConfig.getSerializableIsolation() &&
             txnConfig.getReadCommitted()) ||
            (txnConfig.getReadUncommitted() &&
             txnConfig.getReadCommitted())) {
            throw new IllegalArgumentException
                ("Only one may be specified: SerializableIsolation, " +
                "ReadCommitted or ReadUncommitted");
        }
        if ((txnConfig.getDurability() != null) &&
            ((defaultTxnConfig.getSync() ||
              defaultTxnConfig.getNoSync() ||
              defaultTxnConfig.getWriteNoSync()))) {
           throw new IllegalArgumentException
               ("Mixed use of deprecated durability API for the " +
                "Environment with the new durability API for " +
                "TransactionConfig.setDurability()");
        }
        if ((defaultTxnConfig.getDurability() != null) &&
            ((txnConfig.getSync() ||
              txnConfig.getNoSync() ||
              txnConfig.getWriteNoSync()))) {
            throw new IllegalArgumentException
                   ("Mixed use of new durability API for the " +
                    "Environment with the deprecated durability API for " +
                    "TransactionConfig.");
        }
    }

    /**
     * Synchronously checkpoint the database environment.
     * <p>
     * This is an optional action for the application since this activity
     * is, by default, handled by a database environment owned background
     * thread.
     * <p>
     * A checkpoint has the side effect of flushing all preceding
     * non-transactional write operations, as well as any preceding
     * transactions that were committed with {@link
     * Durability.SyncPolicy#NO_SYNC no-sync durability}.  However, for best
     * performance, checkpoints should be used only to bound recovery time.
     * {@link #flushLog} can be used to write buffered data for durability
     * purposes.
     *
     * @param ckptConfig The checkpoint attributes.  If null, default
     * attributes are used.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void checkpoint(CheckpointConfig ckptConfig)
        throws DatabaseException {

        try {
            checkHandleIsValid();
            checkEnv();
            CheckpointConfig useConfig =
                (ckptConfig == null) ? CheckpointConfig.DEFAULT : ckptConfig;
            envImpl.invokeCheckpoint(useConfig, "api");
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Synchronously flushes database environment databases to stable storage.
     * Calling this method is equivalent to forcing a checkpoint and setting
     * {@link CheckpointConfig#setMinimizeRecoveryTime} to true.
     * <p>
     * A checkpoint has the side effect of flushing all preceding
     * non-transactional write operations, as well as any preceding
     * transactions that were committed with {@link
     * Durability.SyncPolicy#NO_SYNC no-sync durability}.  However, for best
     * performance, checkpoints should be used only to bound recovery time.
     * {@link #flushLog} can be used to write buffered data for durability
     * purposes.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void sync()
        throws DatabaseException {

        try {
            checkHandleIsValid();
            checkEnv();
            CheckpointConfig config = new CheckpointConfig();
            config.setForce(true);
            config.setMinimizeRecoveryTime(true);
            envImpl.invokeCheckpoint(config, "sync");
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Writes buffered data to the log, and optionally performs an fsync to
     * guarantee that data is written to the physical device.
     * <p>
     * This method is used to make durable, by writing to the log, all
     * preceding non-transactional write operations, as well as any preceding
     * transactions that were committed with {@link
     * Durability.SyncPolicy#NO_SYNC no-sync durability}.  If the {@code fsync}
     * parameter is true, it can also be used to flush all logged data to the
     * physical storage device, by performing an fsync.
     * <p>
     * Note that this method <em>does not</em> flush previously unwritten data
     * in deferred-write databases; that is done by calling {@link
     * Database#sync} or performing a checkpoint.
     *
     * @param fsync is true to perform an fsync as well as a file write, or
     * false to perform only a file write.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void flushLog(boolean fsync) {
        try {
            checkHandleIsValid();
            checkEnv();
            envImpl.flushLog(fsync);
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Synchronously invokes database environment log cleaning.  This method is
     * called periodically by the cleaner daemon thread.
     *
     * <p>Zero or more log files will be cleaned as necessary to bring the disk
     * space utilization of the environment above the configured minimum
     * utilization threshold.  The threshold is determined by the {@link
     * EnvironmentConfig#CLEANER_MIN_UTILIZATION} configuration setting.</p>
     *
     * <p>Note that <code>cleanLog</code> does not perform the complete task of
     * cleaning a log file.  Eviction and checkpointing migrate records that
     * are marked by the cleaner, and a full checkpoint is necessary following
     * cleaning before cleaned files will be deleted (or renamed).  Checkpoints
     * normally occur periodically and when the environment is closed.</p>
     *
     * <p>This is an optional action for the application since this activity
     * is, by default, handled by one or more database environment owned
     * background threads.</p>
     *
     * <p>There are two intended use cases for the <code>cleanLog</code>
     * method.  The first case is where the application wishes to disable the
     * built-in cleaner threads using the {@link
     * EnvironmentConfig#ENV_RUN_CLEANER} property.  To replace the
     * functionality of the cleaner threads, the application should call
     * <code>cleanLog</code> periodically.</p>
     *
     * <p>In the second use case, "batch cleaning", the application disables
     * the cleaner threads for maximum performance during active periods, and
     * calls <code>cleanLog</code> during periods when the application is
     * quiescent or less active than usual.  If the cleaner has a large number
     * of files to clean, <code>cleanLog</code> may stop without reaching the
     * target utilization; to ensure that the target utilization is reached,
     * <code>cleanLog</code> should be called in a loop until it returns
     * zero. And to complete the work of cleaning, a checkpoint is necessary.
     * An example of performing batch cleaning follows.</p>
     *
     * <pre>
     *       Environment env;
     *       boolean anyCleaned = false;
     *       while (env.cleanLog() &gt; 0) {
     *           anyCleaned = true;
     *       }
     *       if (anyCleaned) {
     *           CheckpointConfig force = new CheckpointConfig();
     *           force.setForce(true);
     *           env.checkpoint(force);
     *       }
     * </pre>
     *
     * <p><em>WARNING:</em>If batch cleaning (shown above) is performed
     * immediately before closing the environment, then the built-in cleaner
     * threads should normally be disabled using {@link
     * EnvironmentConfig#ENV_RUN_CLEANER} during the batch cleaning process.
     * If the built-in cleaner threads are actively working on one or more log
     * files, then those files will not be processed by the {@code cleanLog}
     * method.  Closing the environment will abort the work being done by the
     * built-in cleaner threads, and log cleaning may be incomplete.</p>
     *
     * @return The number of log files that were cleaned, and that will be
     * deleted (or renamed) when a qualifying checkpoint occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is a read-only or
     * memory-only environment.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public int cleanLog()
        throws DatabaseException {

        try {
            checkHandleIsValid();
            checkEnv();
            return envImpl.invokeCleaner();
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Synchronously invokes the mechanism for keeping memory usage within the
     * cache size boundaries.
     *
     * <p>This is an optional action for the application since this activity
     * is, by default, handled by a database environment owned background
     * thread.</p>
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void evictMemory()
        throws DatabaseException {

        try {
            checkHandleIsValid();
            checkEnv();
            envImpl.invokeEvictor();
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Synchronously invokes the compressor mechanism which compacts in memory
     * data structures after delete operations.
     *
     * <p>This is an optional action for the application since this activity
     * is, by default, handled by a database environment owned background
     * thread.</p>
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void compress()
        throws DatabaseException {

        try {
            checkHandleIsValid();
            checkEnv();
            envImpl.invokeCompressor();
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Preloads the cache with multiple databases.  This method should only be
     * called when there are no operations being performed on the specified
     * databases in other threads.  Executing preload during concurrent updates
     * of the specified databases may result in some or all of the tree being
     * loaded into the JE cache.  Executing preload during any other types of
     * operations may result in JE exceeding its allocated cache
     * size. preload() effectively locks all of the specified database and
     * therefore will lock out the checkpointer, cleaner, and compressor, as
     * well as not allow eviction to occur.
     *
     * @param config The PreloadConfig object that specifies the parameters
     * of the preload.
     *
     * @return A PreloadStats object with the result of the preload operation
     * and various statistics about the preload() operation.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if any of the databases has been closed.
     *
     * @see Database#preload(PreloadConfig)
     */
    public PreloadStats preload(final Database[] databases,
                                final PreloadConfig config)
        throws DatabaseException {

        try {
            checkHandleIsValid();
            checkEnv();
            DatabaseUtil.checkForZeroLengthArrayParam(databases, "databases");
            PreloadConfig useConfig =
                (config == null) ? new PreloadConfig() : config;
            int nDbs = databases.length;
            DatabaseImpl[] dbImpls = new DatabaseImpl[nDbs];
            for (int i = 0; i < nDbs; i += 1) {
                dbImpls[i] = DbInternal.getDatabaseImpl(databases[i]);
            }
            return envImpl.preload(dbImpls, useConfig);
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Returns this object's configuration.
     *
     * @return This object's configuration.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle has been closed.
     */
    public EnvironmentConfig getConfig()
        throws DatabaseException {

        try {
            checkHandleIsValid();
            EnvironmentConfig config = envImpl.cloneConfig();
            handleConfig.copyHandlePropsTo(config);
            config.fillInEnvironmentGeneratedProps(envImpl);
            return config;
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Sets database environment attributes.
     *
     * <p>Attributes only apply to a specific Environment object and are not
     * necessarily shared by other Environment objects accessing this
     * database environment.</p>
     *
     * @param mutableConfig The database environment attributes.  If null,
     * default attributes are used.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle has been closed.
     */
    public synchronized
        void setMutableConfig(EnvironmentMutableConfig mutableConfig)
        throws DatabaseException {

        /*
         * This method is synchronized so that we atomically call both
         * EnvironmentImpl.setMutableConfig and copyToHandleConfig. This
         * ensures that the handle and the EnvironmentImpl properties match.
         */
        try {
            checkHandleIsValid();
            DatabaseUtil.checkForNullParam(mutableConfig, "mutableConfig");

            /*
             * Change the mutable properties specified in the given
             * configuration.
             */
            envImpl.setMutableConfig(mutableConfig);

            /* Reset the handle config properties. */
            copyToHandleConfig(mutableConfig, null, null);
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Returns database environment attributes.
     *
     * @return Environment attributes.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle has been closed.
     */
    public EnvironmentMutableConfig getMutableConfig()
        throws DatabaseException {

        try {
            checkHandleIsValid();
            EnvironmentMutableConfig config =
                envImpl.cloneMutableConfig();
            handleConfig.copyHandlePropsTo(config);
            config.fillInEnvironmentGeneratedProps(envImpl);
            return config;
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Returns the general database environment statistics.
     *
     * @param config The general statistics attributes.  If null, default
     * attributes are used.
     *
     * @return The general database environment statistics.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public EnvironmentStats getStats(StatsConfig config)
        throws DatabaseException {

        checkHandleIsValid();
        checkEnv();
        try {
            StatsConfig useConfig =
                (config == null) ? StatsConfig.DEFAULT : config;

            if (envImpl != null) {
                return envImpl.loadStats(useConfig);
            }
            return new EnvironmentStats();
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Returns the database environment's locking statistics.
     *
     * @param config The locking statistics attributes.  If null, default
     * attributes are used.
     *
     * @return The database environment's locking statistics.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     *
     * @deprecated as of 4.0.10, replaced by {@link
     * Environment#getStats(StatsConfig)}.</p>
     */
    public LockStats getLockStats(StatsConfig config)
        throws DatabaseException {

        try {
            checkHandleIsValid();
            checkEnv();
            StatsConfig useConfig =
                (config == null) ? StatsConfig.DEFAULT : config;

            return envImpl.lockStat(useConfig);
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Returns the database environment's transactional statistics.
     *
     * @param config The transactional statistics attributes.  If null,
     * default attributes are used.
     *
     * @return The database environment's transactional statistics.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public TransactionStats getTransactionStats(StatsConfig config)
        throws DatabaseException {

        try {
            checkHandleIsValid();
            checkEnv();
            StatsConfig useConfig =
                (config == null) ? StatsConfig.DEFAULT : config;
            return envImpl.txnStat(useConfig);
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Returns a List of database names for the database environment.
     *
     * <p>Each element in the list is a String.</p>
     *
     * @return A List of database names for the database environment.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public List<String> getDatabaseNames()
        throws DatabaseException {

        try {
            checkHandleIsValid();
            checkEnv();
            return envImpl.getDbTree().getDbNames();
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Returns if the database environment is consistent and correct.
     *
     * <p>Verification is an expensive operation that should normally only be
     * used for troubleshooting and debugging.</p>
     *
     * @param config The verification attributes.  If null, default
     * attributes are used.
     *
     * @param out The stream to which verification debugging information is
     * written.
     *
     * @return true if the database environment is consistent and correct.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public boolean verify(VerifyConfig config, PrintStream out)
        throws DatabaseException {

        try {
            checkHandleIsValid();
            checkEnv();
            VerifyConfig useConfig =
                (config == null) ? VerifyConfig.DEFAULT : config;
            return envImpl.verify(useConfig, out);
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Returns the transaction associated with this thread if implied
     * transactions are being used.  Implied transactions are used in an XA or
     * JCA "Local Transaction" environment.  In an XA environment the
     * XAEnvironment.start() entrypoint causes a transaction to be created and
     * become associated with the calling thread.  Subsequent API calls
     * implicitly use that transaction.  XAEnvironment.end() causes the
     * transaction to be disassociated with the thread.  In a JCA Local
     * Transaction environment, the call to JEConnectionFactory.getConnection()
     * causes a new transaction to be created and associated with the calling
     * thread.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public Transaction getThreadTransaction()
        throws DatabaseException {

        checkHandleIsValid();
        checkEnv();
        try {
            return envImpl.getTxnManager().getTxnForThread();
        } catch (Error E) {
            if (envImpl != null) {
                envImpl.invalidate(E);
            }
            throw E;
        }
    }

    /**
     * Sets the transaction associated with this thread if implied transactions
     * are being used.  Implied transactions are used in an XA or JCA "Local
     * Transaction" environment.  In an XA environment the
     * XAEnvironment.start() entrypoint causes a transaction to be created and
     * become associated with the calling thread.  Subsequent API calls
     * implicitly use that transaction.  XAEnvironment.end() causes the
     * transaction to be disassociated with the thread.  In a JCA Local
     * Transaction environment, the call to JEConnectionFactory.getConnection()
     * causes a new transaction to be created and associated with the calling
     * thread.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void setThreadTransaction(Transaction txn) {

        checkHandleIsValid();
        checkEnv();
        try {
            envImpl.getTxnManager().setTxnForThread(txn);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns whether this {@code Environment} is open, valid and can be used.
     * If this method returns false, {@link #close} should be called as soon as
     * possible.
     *
     * <p>When an {@link EnvironmentFailureException}, or one of its
     * subclasses, is caught, the {@code isValid} method should be called to
     * determine whether the {@code Environment} can continue to be used, or
     * should be closed.</p>
     */
    public boolean isValid() {
        return envImpl != null &&
            envImpl.isValid();
    }

    /**
     * Print a detailed report about the costs of different phases of
     * environment startup. This report is by default logged to the je.info
     * file if startup takes longer than je.env.startupThreshold.
     */
    public void printStartupInfo(PrintStream out) {
        envImpl.getStartupTracker().displayStats(out, Phase.TOTAL_ENV_OPEN);
    }

    /*
     * Non public api -- helpers
     */

    /**
     * Let the Environment remember what's opened against it.
     */
    private void addReferringHandle(Database db) {
        referringDbs.add(db);
    }

    /**
     * Lets the Environment remember what's opened against it.
     */
    private void addReferringHandle(Transaction txn) {
        referringDbTxns.add(txn);
    }

    /**
     * The referring db has been closed.
     */
    void removeReferringHandle(Database db) {
        referringDbs.remove(db);
    }

    /**
     * The referring Transaction has been closed.
     */
    void removeReferringHandle(Transaction txn) {
        referringDbTxns.remove(txn);
    }

    /**
     * For internal use only.
     * @hidden
     * @throws IllegalStateException if the environment is not open.
     */
    public void checkHandleIsValid()
        throws DatabaseException {

        if (envImpl == null) {
            throw new IllegalStateException
                ("Attempt to use non-open Environment object().");
        }
    }

    /*
     * Debugging aids.
     */

    /**
     * Internal entrypoint.
     */
    EnvironmentImpl getEnvironmentImpl() {
        return envImpl;
    }

    /* Returns true, if this is a handle allocated internally by JE. */
    protected boolean isInternalHandle() {
        return false;
    }

    /**
     * For internal use only.
     * @hidden
     * Throws if the envImpl is invalid.
     */
    protected void checkEnv()
        throws DatabaseException, EnvironmentFailureException {

        if (envImpl == null) {
            return;
        }
        envImpl.checkIfInvalid();
        envImpl.checkNotClosed();
    }

    /**
     * @throws UnsupportedOperationException via the database operation methods
     * (remove, truncate, rename) and potentially other methods that require a
     * writable environment.
     */
    private void checkWritable() {
        if (envImpl.isReadOnly()) {
            throw new UnsupportedOperationException
                ("Environment is Read-Only.");
        }
    }
}
