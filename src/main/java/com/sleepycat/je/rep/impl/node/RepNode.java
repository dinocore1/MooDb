/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.impl.node;

import static com.sleepycat.je.rep.ReplicatedEnvironment.State.DETACHED;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.MASTER;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.REPLICA;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.UNKNOWN;
import static com.sleepycat.je.rep.impl.RepParams.DBTREE_CACHE_CLEAR_COUNT;
import static com.sleepycat.je.rep.impl.RepParams.ENV_CONSISTENCY_TIMEOUT;
import static com.sleepycat.je.rep.impl.RepParams.HEARTBEAT_INTERVAL;
import static com.sleepycat.je.rep.impl.RepParams.LOG_FLUSH_TASK_INTERVAL;
import static com.sleepycat.je.rep.impl.RepParams.RUN_LOG_FLUSH_TASK;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.StartupTracker.Phase;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.rep.AppStateMonitor;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.RestartRequiredException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.elections.Elections;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.TimebasedProposalGenerator;
import com.sleepycat.je.rep.impl.BinaryNodeStateService;
import com.sleepycat.je.rep.impl.GroupService;
import com.sleepycat.je.rep.impl.NodeStateService;
import com.sleepycat.je.rep.impl.PointConsistencyPolicy;
import com.sleepycat.je.rep.impl.RepGroupDB;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.monitor.LeaveGroupEvent.LeaveReason;
import com.sleepycat.je.rep.stream.FeederTxns;
import com.sleepycat.je.rep.stream.MasterChangeListener;
import com.sleepycat.je.rep.stream.MasterStatus;
import com.sleepycat.je.rep.stream.MasterSuggestionGenerator;
import com.sleepycat.je.rep.util.ldiff.LDiffService;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.RepUtils.ExceptionAwareCountDownLatch;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.sync.impl.LogChangeSet;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.VLSN;

/**
 * Represents a replication node. This class is the locus of operations that
 * manage the state of the node, master, replica, etc. Once the state of a node
 * has been established the thread of control passes over to the Replica or
 * FeederManager instances.
 *
 * Note that both Feeders and the Replica instance may be active in future when
 * we support r2r replication, in addition to m2r replication. For now however,
 * either the FeederManager is active, or the Replica is and the same common
 * thread control can be shared between the two.
 */
public class RepNode extends StoppableThread {

    /*
     * The unique node name and internal id that identifies the node within
     * the rep group. There is a canonical instance of this that's updated
     * when the node joins the group.
     */
    private final NameIdPair nameIdPair;

    /*
     * The socket address on which Replicas connect to me, were this node
     * to become the master.
     */
    private final InetSocketAddress mySocket;

    /* The service dispatcher used by this replication node. */
    private final ServiceDispatcher serviceDispatcher;

    /* The election instance for this node */
    private Elections elections;

    /* The locus of operations when the node is a replica. */
    private final Replica replica;

    /* Used when the node is a feeder. */
    private final FeederManager feederManager;

    /*
     * The status of the Master. Note that this is the leading state as
     * communicated to this node via the Listener. The node itself may not as
     * yet have responded to this state change announced by the Listener. That
     * is, nodeState, may reflect a different state until the transition to
     * this state has been completed.
     */
    private final MasterStatus masterStatus;
    private final MasterChangeListener changeListener;
    private final MasterSuggestionGenerator suggestionGenerator;

    /*
     * Represents the application visible state of this node. It may lag the
     * state as described by masterStatus.
     */
    private final NodeState nodeState;

    /*
     * Determines whether a node designated as a Primary is actually active
     * as a Master as a direct result of this designation. If this is true,
     * it indicates that this node is currently the master in a two node group
     * and that it's not in communication with the Secondary node.
     */
    private volatile boolean activePrimary = false;

    /*
     * If non-zero use this value overrides the normal group size calculations.
     */
    private int electableGroupSizeOverride;

    private final RepImpl repImpl;

    /* The encapsulated internal replication group database. */
    final RepGroupDB repGroupDB;

    /*
     * The latch used to indicate that the node has a well defined state as a
     * Master or Replica and has finished the node-specific initialization that
     * will permit it to function immediately in that capacity.
     *
     * For a Master it means that it's ready to start accepting connections
     * from Replicas.
     *
     * For a Replica, it means that it has established a connection with a
     * Feeder, completed the handshake process that validates it as being a
     * legitimate member of the group, established a sync point, and is ready
     * to start replaying the replication stream.
     */
    private volatile ExceptionAwareCountDownLatch readyLatch = null;

    /*
     * Latch used to freeze txn commit VLSN advancement during an election.
     */
    private final CommitFreezeLatch vlsnFreezeLatch = new CommitFreezeLatch();

    /*
     * Describes the nodes that form the group. This information is dynamic
     * it's initialized at startup and subsequently as a result of changes
     * made either directly to it, when the node is a master, or via the
     * replication stream, when it is a Replica.
     */
    private RepGroupImpl group;

    /*
     * Represents the VLSN of TxnAbort/TxnCommit entry of the last finished
     * transaction. It's volatile so that reads initiated by the feeders get
     * the most uptodate values.
     */
    volatile private VLSN currentTxnEndVLSN = null;

    /*
     * Determines the election policy to use when the node holds its very first
     * elections
     */
    private QuorumPolicy electionQuorumPolicy = QuorumPolicy.SIMPLE_MAJORITY;

    /*
     * Amount of times to sleep between retries when a new node tries to locate
     * a master.
     */
    private static final int MASTER_QUERY_INTERVAL = 10000;

    /* Number of times to retry joining on a retryable exception. */
    private static final int JOIN_RETRIES = 10;

    /*
     * Encapsulates access to current time, to arrange for testing of clock
     * skews.
     */
    private final Clock clock;

    private com.sleepycat.je.rep.impl.networkRestore.FeederManager
        logFeederManager;
    private LDiffService ldiff;
    private NodeStateService nodeStateService;
    private BinaryNodeStateService binaryNodeStateService;

    /* tracks the local CBVLSN for this node. */
    final LocalCBVLSNTracker cbvlsnTracker;

    /* The currently in-progress Master Transfer operation, if any. */
    private MasterTransfer xfrInProgress;

    /* calculates and manages the global, cached CBVLSN */
    final GlobalCBVLSN globalCBVLSN;

    /* Determines how long to wait for a replica to catch up on a close. */
    private long replicaCloseCatchupMs = -1;

    /* Manage and notify MonitorChangeEvents fired by this RepNode. */
    private MonitorEventManager monitorEventManager;

    /* The user defined AppStateMonitor which gets the application state. */
    private AppStateMonitor appStateMonitor;

    /* A timer used to track inactive socket channels used by the RepNode. */
    private final Timer timer;
    private final ChannelTimeoutTask channelTimeoutTask;
    private LogFlusher logFlusher;

    final Logger logger;

    /* Used by tests only. */
    private int logVersion = LogEntryType.LOG_VERSION;

    public RepNode(RepImpl repImpl,
                   Replay replay,
                   NodeState nodeState)
        throws IOException, DatabaseException {

        super(repImpl, "RepNode " + repImpl.getNameIdPair());

        this.repImpl = repImpl;
        readyLatch = new ExceptionAwareCountDownLatch(repImpl, 1);
        nameIdPair = repImpl.getNameIdPair();
        logger = LoggerUtils.getLogger(getClass());

        this.mySocket = repImpl.getSocket();
        this.serviceDispatcher = new ServiceDispatcher(mySocket, repImpl);
        serviceDispatcher.start();
        clock = new Clock(RepImpl.getClockSkewMs());
        this.repGroupDB = new RepGroupDB(repImpl);

        masterStatus = new MasterStatus(nameIdPair);
        replica = ReplicaFactory.create(this, replay);

        feederManager = new FeederManager(this);
        changeListener = new MasterChangeListener(this);
        suggestionGenerator = new MasterSuggestionGenerator(this);

        this.nodeState = nodeState;

        electableGroupSizeOverride = repImpl.getConfigManager().
            getInt(RepParams.ELECTABLE_GROUP_SIZE_OVERRIDE);
        if (electableGroupSizeOverride > 0) {
            LoggerUtils.warning(logger, repImpl,
                                "Electable group size override set to:" +
                                electableGroupSizeOverride);
        }

        utilityServicesStart();
        this.cbvlsnTracker = new LocalCBVLSNTracker(this);
        this.globalCBVLSN = new GlobalCBVLSN(this);
        this.monitorEventManager = new MonitorEventManager(this);
        timer = new Timer(true);
        channelTimeoutTask = new ChannelTimeoutTask(timer);
        configLogFlusher(getConfigManager());
    }

    private void utilityServicesStart() {
        ldiff = new LDiffService(serviceDispatcher, repImpl);
        logFeederManager =
            new com.sleepycat.je.rep.impl.networkRestore.FeederManager
            (serviceDispatcher, repImpl, nameIdPair);

        /* Register the node state querying service. */
        nodeStateService = new NodeStateService(serviceDispatcher, this);
        serviceDispatcher.register(nodeStateService);

        binaryNodeStateService =
            new BinaryNodeStateService(serviceDispatcher, this);
    }

    /* Create a placeholder node, for test purposes only. */
    public RepNode(NameIdPair nameIdPair) {
        this(nameIdPair, null);
    }

    public RepNode() {
        this(NameIdPair.NULL);
    }

    public RepNode(NameIdPair nameIdPair,
                   ServiceDispatcher serviceDispatcher) {
        super("RepNode " + nameIdPair);
        repImpl = null;
        clock = new Clock(0);

        this.nameIdPair = nameIdPair;
        mySocket = null;
        this.serviceDispatcher = serviceDispatcher;

        this.repGroupDB = null;

        masterStatus = new MasterStatus(NameIdPair.NULL);
        replica = null;
        feederManager = null;
        changeListener = null;
        suggestionGenerator = null;
        nodeState = null;
        cbvlsnTracker = null;
        globalCBVLSN = null;
        logger = null;
        timer = null;
        channelTimeoutTask = null;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    /**
     * Returns the timer associated with this RepNode
     */
    public Timer getTimer() {
        return timer;
    }

    public ServiceDispatcher getServiceDispatcher() {
        return serviceDispatcher;
    }

    /**
     * Returns the accumulated statistics for this node. The method
     * encapsulates the statistics associated with its two principal components
     * the FeederManager and the Replica.
     */
    public ReplicatedEnvironmentStats getStats(StatsConfig config) {
        return RepInternal.makeReplicatedEnvironmentStats(repImpl, config);
    }

    public void resetStats() {
        feederManager.resetStats();
        replica.resetStats();
    }

    public ExceptionAwareCountDownLatch getReadyLatch() {
        return readyLatch;
    }

    public CommitFreezeLatch getVLSNFreezeLatch() {
        return vlsnFreezeLatch;
    }

    public void resetReadyLatch(Exception exception) {
        if (readyLatch.getCount() != 0) {
            /* releasing latch in some error situation. */
            readyLatch.releaseAwait(exception);
        }
        readyLatch = new ExceptionAwareCountDownLatch(repImpl, 1);
    }

    /* The methods below return the components of the rep node. */
    public FeederManager feederManager() {
        return feederManager;
    }

    public Replica replica() {
        return replica;
    }

    public Clock getClock() {
        return clock;
    }

    public Replica getReplica() {
        return replica;
    }

    public RepGroupDB getRepGroupDB() {
        return repGroupDB;
    }

    public RepGroupImpl getGroup() {
        return group;
    }

    /**
     * Returns the UUID associated with the replicated environment.
     */
    public UUID getUUID() {
        if (group == null) {
            throw EnvironmentFailureException.unexpectedState
                ("Group info is not available");
        }
        return group.getUUID();
    }

    /**
     * Returns the nodeName associated with this replication node.
     *
     * @return the nodeName
     */
    public String getNodeName() {
        return nameIdPair.getName();
    }

    /**
     * Returns the nodeId associated with this replication node.
     *
     * @return the nodeId
     */
    public int getNodeId() {
        return nameIdPair.getId();
    }

    public NameIdPair getNameIdPair() {
        return nameIdPair;
    }

    public InetSocketAddress getSocket() {
        return mySocket;
    }

    public MasterStatus getMasterStatus() {
        return masterStatus;
    }

    /**
     * Returns a definitive answer to whether this node is currently the master
     * by checking both its status as a master and that at least a simple
     * majority of nodes agrees that it's the master based on the number of
     * feeder connections to it. Such an authoritative answer is needed in a
     * network partition situation to detect a master that may be isolated on
     * the minority side of a network partition.
     *
     * @return true if the node is definitely the master. False if it's not or
     * we cannot be sure.
     */
    public boolean isAuthoritativeMaster() {
        if (!getMasterStatus().isGroupMaster()) {
            return false;
        }

        return (feederManager.activeReplicaCount() + 1) >=
               getElectionQuorumSize(QuorumPolicy.SIMPLE_MAJORITY);
    }

    public int getHeartbeatInterval() {
        return getConfigManager().getInt(HEARTBEAT_INTERVAL);
    }

    /* For unit testing only. */
    public void setVersion(int version) {
        logVersion = version;
    }

    public int getLogVersion() {
        return logVersion;
    }

    public int getElectionPriority() {
        final int priority =
            getConfigManager().getInt(RepParams.NODE_PRIORITY);
        final int defaultPriority =
            Integer.parseInt(RepParams.NODE_PRIORITY.getDefault());
        return (getConfigManager().getBoolean(RepParams.DESIGNATED_PRIMARY) &&
                (priority == defaultPriority)) ?
            defaultPriority + 1 : /* Raise its priority. */
            priority; /* Explicit priority, leave it intact. */
    }

    /*
     * Amount of time to wait for a thread to finish on a shutdown. It's
     * a multiple of a heartbeat, since a thread typically polls for a
     * shutdown once per heartbeat.
     */
    public int getThreadWaitInterval() {
        return getHeartbeatInterval()*4;
    }

    int getDbTreeCacheClearingOpCount() {
        return getConfigManager().getInt(DBTREE_CACHE_CLEAR_COUNT);
    }

    public RepImpl getRepImpl() {
        return repImpl;
    }

    public LogManager getLogManager() {
        return repImpl.getLogManager();
    }

    DbConfigManager getConfigManager() {
        return repImpl.getConfigManager();
    }

    public VLSNIndex getVLSNIndex() {
        return repImpl.getVLSNIndex();
    }

    public FeederTxns getFeederTxns() {
        return repImpl.getFeederTxns();
    }

    public Elections getElections() {
        return elections;
    }

    public MasterSuggestionGenerator getSuggestionGenerator() {
        return suggestionGenerator;
    }

    /* Used by unit tests only. */
    public QuorumPolicy getElectionPolicy() {
        return electionQuorumPolicy;
    }

    /**
     * Returns a list of nodes suitable for feeding log files for a network
     * restore.
     *
     * @return a list of hostPort pairs
     */
    public RepNodeImpl[] getLogProviders() {
        Set<RepNodeImpl> nodes = getGroup().getAllElectableMembers();
        RepNodeImpl[] logProviders = new RepNodeImpl[nodes.size()];
        int i=0;
        for (RepNodeImpl node : nodes) {
            logProviders[i++] = node;
        }
        return logProviders;
    }

    /* Used by unit tests only. */
    public LogFlusher getLogFlusher() {
        return logFlusher;
    }

    /* Configure the log flusher according to the configuration changes. */
    public void configLogFlusher(DbConfigManager configMgr) {
        boolean enableTask = configMgr.getBoolean(RUN_LOG_FLUSH_TASK);
        int flushInterval = configMgr.getDuration(LOG_FLUSH_TASK_INTERVAL);

        /* Cancel the log flushing the task if we want to. */
        if (!enableTask) {
            if (logFlusher != null) {
                logFlusher.cancelTask();
            }

            return;
        }

        /* Create LogFlusher if it's null and we do want to start the task. */
        if (logFlusher == null) {
            logFlusher = new LogFlusher(this, timer);
        }

        /* Configure the flushing task. */
        logFlusher.configFlushTask(flushInterval);
    }

    public ChannelTimeoutTask getChannelTimeoutTask() {
        return channelTimeoutTask;
    }

    public boolean isMaster() {
        return masterStatus.isNodeMaster();
    }

    /**
     * Notes the VLSN associated with the latest commit or abort. The updates
     * are done in ascending order.
     *
     * @param txnEndVLSN the VLSN of a TxnAbort/TxnCommit
     */
    public void currentTxnEndVLSN(VLSN txnEndVLSN) {
        currentTxnEndVLSN = txnEndVLSN;
    }

    public MonitorEventManager getMonitorEventManager() {
        return monitorEventManager;
    }

    /**
     * Register an AppStateMonitor with this RepNode.
     */
    public void registerAppStateMonitor(AppStateMonitor stateMonitor) {
        this.appStateMonitor = stateMonitor;
    }

    /**
     * Return the application state that defined in user specified
     * AppStateMonitor.
     */
    public byte[] getAppState() {

        /*
         * If the AppStateMonitor is not defined, or there is currently no
         * returned application state, return null.
         */
        if (appStateMonitor == null || appStateMonitor.getAppState() == null) {
            return null;
        }

        /* Application state shouldn't be a zero length byte array. */
        if (appStateMonitor.getAppState().length == 0) {
            throw new IllegalStateException
                ("Application state should be a byte array larger than 0.");
        }

        return appStateMonitor.getAppState();
    }

    /* Get the current master name if it exists. */
    public String getMasterName() {
        if (masterStatus.getGroupMasterNameId().getId() ==
            NameIdPair.NULL_NODE_ID) {
            return null;
        }

        return masterStatus.getGroupMasterNameId().getName();
    }

    /**
     * Returns the latest VLSN associated with a replicated commit.
     */
    public VLSN getCurrentTxnEndVLSN() {
        return currentTxnEndVLSN;
    }

    /*
     * Testing API used to force this node as a master. The mastership is
     * communicated upon election completion via the Listener. It's the
     * responsibility of the caller to ensure that only one node is forced
     * at a time via this API.
     *
     * @param force true to force this node as the master, false reverts back
     *              to use of normal (non-preemptive) elections.
     */
    public void forceMaster(boolean force)
        throws InterruptedException, DatabaseException {

        suggestionGenerator.forceMaster(force);
        /* Initiate elections to make the changed proposal heard. */
        refreshCachedGroup();
        elections.initiateElection(group, electionQuorumPolicy);
    }

    /**
     * Starts up the thread in which the node does its processing as a master
     * or replica. It then waits for the newly started thread to transition it
     * out of the DETACHED state, and returns upon completion of this
     * transition.
     *
     * @throws IOException
     * @throws DatabaseException
     */
    private void startup(QuorumPolicy initialElectionPolicy)
        throws IOException, DatabaseException {

        if (isAlive()) {
            return;
        }

        assert(nodeState.getRepEnvState().isDetached());
        elections = new Elections(this,
                                  changeListener,
                                  suggestionGenerator);

        repImpl.getStartupTracker().start(Phase.FIND_MASTER);
        try {

            if (repImpl.getConfigManager().
                getBoolean(RepParams.RESET_REP_GROUP)) {
                /* Invoked by DbResetRepGroup utility */
                reinitSelfElect();
            } else {
                findMaster();
            }
            this.electionQuorumPolicy = initialElectionPolicy;
            elections.participate();
        } finally {
            repImpl.getStartupTracker().stop(Phase.FIND_MASTER);
        }

        start();
    }

    /**
     * This method must be invoked when a RepNode is first initialized and
     * subsequently every time there is a change to the replication group.
     * <p>
     * The Master should invoke this method each time a member is added or
     * removed, and a replica should invoke it each time it detects the commit
     * of a transaction that modifies the membership database.
     * <p>
     * In addition, it must be invoked after a syncup operation, since it may
     * revert changes made to the membership table.
     *
     * @throws DatabaseException
     */
    public RepGroupImpl refreshCachedGroup()
        throws DatabaseException {

        group = repGroupDB.getGroup(new NoConsistencyRequiredPolicy());
        elections.updateRepGroup(group);
        if (nameIdPair.hasNullId()) {
            RepNodeImpl n = group.getMember(nameIdPair.getName());
            if (n != null) {
                /* May not be sufficiently current in the rep stream. */
                nameIdPair.update(n.getNameIdPair());
            }
        }
        return group;
    }

    /**
     * Removes a node so that it's no longer a member of the group.
     *
     * Note that names referring to deleted nodes cannot be reused.
     *
     * @param nodeName identifies the node to be deleted.
     *
     * @throws MemberNotFoundException if the node denoted by
     * <code>memberName</code> is not a member of the replication group.
     *
     * @throws MasterStateException if the member being removed is currently
     * the Master
     *
     * @see <a href="https://sleepycat.oracle.com/trac/wiki/DynamicGroupMembership#DeletingMembers">Member Deletion</a>
     */
    public void removeMember(String nodeName) {
        checkValidity(nodeName, "Removing member");

        /*
         * First remove it from the cached group, effectively setting new
         * durability requirements, for the ensuing group db updates.
         */
        RepNodeImpl node = group.removeMember(nodeName);

        /*
         * Shutdown any feeder that may be active with the replica. Any
         * subsequent attempts by the replica to rejoin the group will result
         * in a failure.
         */
        feederManager.shutdownFeeder(node);
        repGroupDB.removeMember(node);
    }

    /**
     * Update the network address of a node.
     *
     * Note that an alive node's address can't be updated, we'll throw an
     * ReplicaStateException for this case.
     *
     * @param nodeName identifies the node to be updated
     * @param newHostName the new host name of this node
     * @param newPort the new port of this node
     */
    public void updateAddress(String nodeName,
                              String newHostName,
                              int newPort) {
        checkValidity(nodeName, "Updating node's address");

        /* Check whether the node is still alive. */
        if (feederManager.getFeeder(nodeName) != null) {
            throw new ReplicaStateException
                ("Can't update the network address for a live node.");
        }

        /* Update the node information in the group database. */
        RepNodeImpl node = group.getNode(nodeName);
        node.setHostName(newHostName);
        node.setPort(newPort);
        repGroupDB.updateMember(node);
    }

    /**
     * Transfer the master role to one of the specified replicas.
     * <p>
     * We delegate most of the real work to an instance of the {@link
     * MasterTransfer} class.  Here, after some simple initial validity
     * checking, we're concerned with coordinating the potential for multiple
     * overlapping Master Transfer operation attempts.  The possible outcomes
     * are:
     * <ol>
     * <li>complete success ({@code done == true})
     * <ul>
     * <li>
     * don't unblock txns; that'll happen automatically as part of the usual
     * handling of environment invalidation upon producing the
     * MasterReplicaTransitionException
     * <li>
     * don't clear xfrInProgress, because we don't want to allow another
     * attempt to supersede
     * </ul>
     * <li>timeout before establishing a winner (no superseder)
     * <ul>
     * <li>unblock txns
     * <li>clear xfrInProgress
     * </ul>
     * <li>superseded (see {@link #setUpTransfer})
     * <ul>
     * <li>abort existing op (if permitted), unblock txns before unleashing the
     * new one
     * <li>replace xfrInProgress
     * </ul>
     * <li>env is closed (or invalidated because of an error) during the
     * operation
     * <ul>
     * <li>release the block
     * <li>leave xfrInProgress as is.
     * </ul>
     * </ol>
     *
     * @param replicas candidate targets for new master role
     * @param timeout time limit, in msec
     * @param force whether to replace any existing, in-progress
     * transfer operation
     */
    public String transferMaster(Set<String> replicas,
                                 long timeout,
                                 boolean force) {
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException
                ("Parameter 'replicas' cannot be null or empty");
        }
        if (!nodeState.getRepEnvState().isMaster()) {
            throw new IllegalStateException("Not currently master");
        }
        if (replicas.contains(getNodeName())) {

            /*
             * The local node is on the list of candidate new masters, and
             * we're already master: the operation is trivially satisfied.
             */
            return getNodeName();
        }
        for (String replica : replicas) {
            RepNodeImpl node = group.getNode(replica);
            if (node == null || node.isRemoved()) {
                throw new IllegalArgumentException
                    ("Node '" + replica +
                     "' is not currently an active member of the group");
            }
        }

        MasterTransfer xfr = setUpTransfer(replicas, timeout, force);
        boolean done = false;
        try {
            String winner = xfr.transfer();
            done = true;
            return winner;
        } finally {
            synchronized (this) {
                if (xfrInProgress == xfr && !done) {
                    xfrInProgress = null;
                }
            }
        }
    }

    /**
     * Sets up a Master Transfer operation, ensuring that only one operation
     * can be in progress at a time.
     */
    synchronized private MasterTransfer setUpTransfer(Set<String> replicas,
                                                      long timeout,
                                                      boolean force) {
        boolean reject = false; // initial guess, refine below if nec.
        if (xfrInProgress != null) {
            reject = true;      // next best guess, refine below again if nec.

            /*
             * If the new operation is "forcing", see if we can abort the
             * existing one.
             */
            if (force &&
                xfrInProgress.abort
                (new MasterTransferFailureException("superseded"))) {
                reject = false;

                repImpl.unblockTxnCompletion();
            }
        }
        if (reject) {
            throw new MasterTransferFailureException
                ("another Master Transfer (started at " +
                 new Date(xfrInProgress.getStartTime()) +
                 ") is already in progress");
        }
        xfrInProgress = new MasterTransfer(replicas, timeout, this);
        return xfrInProgress;
    }

    public MasterTransfer getActiveTransfer() {
        return xfrInProgress;
    }

    /**
     * Performs some basic validity checking, common code for some
     * Group Membership operations.
     *
     * @param nodeName name of a replica node on which an operation is
     * to be performed
     * @param actionName textual description of the operation (for
     * exception message)
     */
    private void checkValidity(String nodeName, String actionName)
        throws MemberNotFoundException {

        if (!nodeState.getRepEnvState().isMaster()) {
            throw EnvironmentFailureException.unexpectedState
                ("Not currently a master. " + actionName + " must be " +
                 "invoked on the node that's currently the master.");
        }

        RepNodeImpl node = group.getNode(nodeName);
        if (node == null) {
            throw new MemberNotFoundException("Node:" + nodeName +
                                              "is not a member of the group:" +
                                              group.getName());
        }

        if (node.isRemoved() && node.isQuorumAck()) {
            throw new MemberNotFoundException("Node:" + nodeName +
                                              "is not currently a member of " +
                                              "the group:" + group.getName() +
                                              " It had been removed.");
        }

        /* Check if the node is the master itself. */
        if (nodeName.equals(getNodeName())) {
            throw new MasterStateException(getRepImpl().
                                           getStateChangeEvent());
        }
    }

    /**
     * Updates the cached group info for the node, avoiding a database read.
     *
     * @param updateNameIdPair the node whose localCBVLSN must be updated.
     * @param barrierState the new node syncup state
     */
    public void updateGroupInfo(NameIdPair updateNameIdPair,
                                RepGroupImpl.BarrierState barrierState) {

        RepNodeImpl node = group.getMember(updateNameIdPair.getName());
        if (node == null) {
            /*  A subsequent refresh will get it, along with the new node. */
            return;
        }

        LoggerUtils.fine(logger, repImpl,
                         "LocalCBVLSN for " + updateNameIdPair +
                         " updated to " + barrierState +
                         " from " + node.getBarrierState().getLastCBVLSN());
        node.setBarrierState(barrierState);
        globalCBVLSN.recalculate(group);
    }

    /**
     * Recalculate the Global CBVLSN, provoked by Replay, to ensure that the
     * replica's global CBVLSN is up to date.
     */
    void recalculateGlobalCBVLSN() {
        globalCBVLSN.recalculate(group);
    }

    LocalCBVLSNTracker getCBVLSNTracker() {
        return cbvlsnTracker;
    }

    public void freezeLocalCBVLSN() {
        cbvlsnTracker.incrementFreezeCounter();
    }

    public void unfreezeLocalCBVLSN() {
        cbvlsnTracker.decrementFreezeCounter();
    }

    /**
     * Finds a master node.
     *
     * @throws IOException
     * @throws DatabaseException
     * @throws InterruptedException
     */
    private void findMaster()
        throws IOException,
               DatabaseException {

        refreshCachedGroup();
        elections.startLearner();
        LoggerUtils.info(logger, repImpl, "Current group size: " +
                         group.getElectableGroupSize());
        RepNodeImpl thisNode = group.getNode(nameIdPair.getName());
        if (thisNode == null) {
            LoggerUtils.info(logger, repImpl, "New node " + nameIdPair +
                             " unknown to rep group");
            Set<InetSocketAddress> helperSockets = repImpl.getHelperSockets();

            /*
             * Not present in the replication group. Use the helper, to get
             * to a master and enter the group.
             */
            if ((group.getElectableGroupSize() == 0) &&
                (helperSockets.size() == 1) &&
                 serviceDispatcher.getSocketAddress().
                     equals(helperSockets.iterator().next())) {
                /* A startup situation, should this node become master. */
                selfElect();
                elections.updateRepGroup(group);
                return;
            }
            queryGroupForMaster();
        } else {
            /* The node is in the group database. */
            if (thisNode.isRemoved()) {
                throw EnvironmentFailureException.unexpectedState
                    ("Node: " + nameIdPair.getName() +
                     " was previously deleted.");
            }
            LoggerUtils.info(logger, repImpl,
                             "Existing node " + nameIdPair.getName() +
                             " querying for a current master.");

            /*
             * The group has other members, see if they know of a master,
             * along with any helpers that were also supplied.
             */
            Set<InetSocketAddress> helperSockets = repImpl.getHelperSockets();
            helperSockets.addAll(group.getLearnerSockets());
            elections.getLearner().queryForMaster(helperSockets);
        }
    }

    /**
     * This method enforces the requirement that all addresses within a
     * replication group, must be loopback addresses or they must all be
     * non-local ip addresses. Mixing them means that the node with a loopback
     * address cannot be contacted by a different node.
     *
     * @param helperSockets the helper nodes used by this node when contacting
     * the master.
     */
    private void checkLoopbackAddresses(Set<InetSocketAddress> helperSockets) {

        final InetAddress myAddress = mySocket.getAddress();
        final boolean isLoopback= myAddress.isLoopbackAddress();

        for (InetSocketAddress socketAddress : helperSockets) {
            final InetAddress nodeAddress = socketAddress.getAddress();

            if (nodeAddress.isLoopbackAddress() ==  isLoopback) {
                continue;
            }
            String message = mySocket +
                " the address associated with this node, " +
                (isLoopback? "is " : "is not ") +  "a loopback address." +
                " It conflicts with an existing use, by a different node " +
                " of the address:" +
                socketAddress +
                (!isLoopback ? " which is a loopback address." :
                 " which is not a loopback address.") +
                " Such mixing of addresses within a group is not allowed, " +
                "since the nodes will not be able to communicate with " +
                "each other.";
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Used by a new node (that is not a self-elected master) to identify a
     * master. A new node, one that is not as yet in group database, queries
     * the designated helpers and all known learners for the current master.
     * The helpers are the ones that were identified via the node's
     * configuration, while the learners are the ones currently in the member
     * database. It uses both to cast the widest possible net.
     */
    private void queryGroupForMaster() {
        Set<InetSocketAddress> helperSockets = repImpl.getHelperSockets();

        checkLoopbackAddresses(helperSockets);

        /*
         * Not in the rep group. Use the designated helpers and other members
         * of the group to detect a master.
         */
        Set<InetSocketAddress> learners =
            new HashSet<InetSocketAddress>(helperSockets);
        learners.addAll(group.getLearnerSockets());
        if (learners.size() == 0) {
            throw EnvironmentFailureException.unexpectedState
                ("Need a helper to add a new node into the group");
        }
        while (true) {
            elections.getLearner().queryForMaster(learners);
            if (masterStatus.getGroupMasterNameId().getId() !=
                NameIdPair.NULL_NODE_ID) {
                break;
            }

            try {
                Thread.sleep(MASTER_QUERY_INTERVAL);
            } catch (InterruptedException e) {
                throw EnvironmentFailureException.unexpectedException(e);
            }
        }
        LoggerUtils.info(logger, repImpl, "New node " + nameIdPair.getName() +
                         " located master: " +
                         masterStatus.getGroupMasterNameId());
    }

    /**
     * Elects this node as the master. The operation is only valid when the
     * group consists of just this node.
     * @param helperLearner
     * @throws DatabaseException
     */
    private void selfElect()
        throws DatabaseException {

        nameIdPair.setId(RepGroupImpl.getFirstNodeId());

        /* Master by default of a nascent group. */
        Proposal proposal = new TimebasedProposalGenerator().nextProposal();
        elections.getLearner().processResult(proposal,
                                             suggestionGenerator.get(proposal));
        LoggerUtils.info(logger, repImpl, "Nascent group. " +
                         nameIdPair.getName() +
                         " is master by virtue of being the first node.");
        nodeState.changeAndNotify(UNKNOWN, NameIdPair.NULL);
        masterStatus.sync();
        nodeState.changeAndNotify(MASTER, masterStatus.getNodeMasterNameId());
        repImpl.getVLSNIndex().initAsMaster();
        repGroupDB.addFirstNode();
        refreshCachedGroup();
        /* Unsync so that the run loop does not call for an election. */
        masterStatus.unSync();
    }

    /**
     * Establishes this node as the master, after re-initializing the group
     * with this as the sole node in the group. This method is used solely
     * as part of the DbResetrepGroup utility.
     */
    void reinitSelfElect()
        throws IOException {

        /* Establish an empty group so transaction commits can proceed. */
        group = repGroupDB.emptyGroup;
        LoggerUtils.info(logger, repImpl, "Reinitializing group to node " +
                         nameIdPair);

        /*
         * Unilaterally transition the nodeState to Master, so that write
         * transactions needed to reset the group and establish this node can
         * be issued against the environment.
         */
        nodeState.changeAndNotify(UNKNOWN, NameIdPair.NULL);
        nodeState.changeAndNotify(MASTER, masterStatus.getNodeMasterNameId());
        repImpl.getVLSNIndex().initAsMaster();

        /*
         * Start using new log files. The file ensures that we can safely
         * truncate the past VLSNs.
         */
        repImpl.forceLogFileFlip();

        CheckpointConfig ckptConfig = new CheckpointConfig();
        ckptConfig.setForce(true);

        /*
         * The checkpoint ensures that we do not have to replay VLSNs from the
         * prior group and that we have a complete VLSN index on disk.
         */
        repImpl.getCheckpointer().doCheckpoint(ckptConfig,
                                               "Reinit of RepGroup");
        VLSN lastOldVLSN = repImpl.getVLSNIndex().getRange().getLast();

        /* Now create the new rep group on disk. */
        repGroupDB.reinitFirstNode(lastOldVLSN);
        refreshCachedGroup();

        long lastOldFile =
            repImpl.getVLSNIndex().getLTEFileNumber(lastOldVLSN);

        /*
         * Discard the VLSN index covering the pre group reset VLSNS, to ensure
         * that the pre reset part of the log is never replayed. We don't want
         * to replay this part of the log, since it contains references to
         * repnodes via node ids that are no longer part of the reset rep
         * group. Note that we do not reuse rep node ids, that is, rep node id
         * sequence continues across the reset operation and is not itself
         * reset. Nodes joining the new group will need to do a network restore
         * when they join the group.
         */
        repImpl.getVLSNIndex().truncateFromHead(lastOldVLSN, lastOldFile);

        elections.startLearner();
        /* Unsync so that the run loop does not call for an election. */
        masterStatus.unSync();
    }

    /**
     * The top level Master/Feeder or Replica loop in support of replication.
     * It's responsible for driving the node level state changes resulting
     * from elections initiated either by this node, or by other members of the
     * group.
     * <p>
     * The thread is terminated via an orderly shutdown initiated as a result
     * of an interrupt issued by the shutdown() method. Any exception that is
     * not handled by the run method itself is caught by the thread's uncaught
     * exception handler, and results in the RepImpl being made invalid.  In
     * that case, the application is responsible for closing the Replicated
     * Environment, which will provoke the shutdown.
     * <p>
     * Note: This method currently runs either the feeder loop or the replica
     * loop. With R to R support, it would be possible for a Replica to run
     * both. This will be a future feature.
     */
    @Override
    public void run() {
        if (nodeState.getRepEnvState().isDetached()) {
            nodeState.changeAndNotify(UNKNOWN, NameIdPair.NULL);
        }

        /* Set to indicate an error-initiated shutdown. */
        Error repNodeError = null;
        try {
            LoggerUtils.info(logger, repImpl,
                             "Node " + nameIdPair.getName() + " started");
            while (!isShutdownOrInvalid()) {
                if (nodeState.getRepEnvState() != UNKNOWN) {
                    /* Avoid unnecessary state changes. */
                    nodeState.changeAndNotify(UNKNOWN, NameIdPair.NULL);
                }

                /*
                 * Initiate elections if we don't have a group master, or there
                 * is a master, but we were unable to use it.
                 */
                if (masterStatus.getGroupMasterNameId().hasNullId() ||
                    masterStatus.inSync()) {

                    elections.initiateElection(group, electionQuorumPolicy);

                    /*
                     * Subsequent elections must always use a simple majority.
                     */
                    electionQuorumPolicy = QuorumPolicy.SIMPLE_MAJORITY;
                    /* In case elections were shutdown. */
                    if (isShutdownOrInvalid()) {
                        return;
                    }
                }

                /* Start syncing this node to the new group master */
                masterStatus.sync();
                /* Copy status to hold it stable against concurrent updates. */
                MasterStatus status = (MasterStatus) masterStatus.clone();

                if (status.isNodeMaster()) {
                    repImpl.getVLSNIndex().initAsMaster();
                    replica.masterTransitionCleanup();
                    try {
                        serviceDispatcher.register
                            (new GroupService(serviceDispatcher, this));
                        /* Master is ready for business. */
                        nodeState.changeAndNotify
                            (MASTER, status.getNodeMasterNameId());
                        feederManager.runFeeders();
                    } finally {
                        serviceDispatcher.cancel(GroupService.SERVICE_NAME);
                    }
                } else {
                    nodeState.changeAndNotify
                        (REPLICA, status.getNodeMasterNameId());
                    replica.runReplicaLoop();
                }
            }
        } catch (InterruptedException e) {
            LoggerUtils.fine(logger, repImpl,
                             "RepNode main thread interrupted - " +
                             " forced shutdown.");
        } catch (GroupShutdownException e) {
            saveShutdownException(e);
        } catch (RuntimeException e) {
            saveShutdownException(e);
            throw e;
        } catch (Error e) {
            repNodeError = e;
            repImpl.invalidate(e);
        } finally {
            try {
                LoggerUtils.info(logger, repImpl,
                                 "RepNode main thread shutting down.");

                if (repNodeError != null) {
                    LoggerUtils.info(logger, repImpl,
                                     "Node state at shutdown:\n"+
                                     repImpl.dumpState());
                    throw repNodeError;
                }
                Throwable exception = getSavedShutdownException();

                if (exception == null) {
                    LoggerUtils.fine(logger, repImpl,
                                     "Node state at shutdown:\n"+
                                     repImpl.dumpState());
                } else {
                    LoggerUtils.info(logger, repImpl,
                                     "RepNode shutdown exception:\n" +
                                     exception.getMessage() +
                                     repImpl.dumpState());
                }

                try {
                    shutdown();
                } catch (DatabaseException e) {
                    RepUtils.chainExceptionCause(e, exception);
                    LoggerUtils.severe(logger, repImpl,
                                       "Unexpected exception during shutdown" +
                                       e);
                    throw e;
                }
            } catch (InterruptedException e1) {
                // Ignore exceptions on exit
            }
            nodeState.changeAndNotify(DETACHED, NameIdPair.NULL);
            cleanup();
        }
    }

    private boolean isShutdownOrInvalid() {
        if (isShutdown()) {
            return true;
        }
        if (getRepImpl().isInvalid()) {
            saveShutdownException(getRepImpl().getInvalidatingException());
            return true;
        }
        return false;
    }

    /**
     * Used to shutdown all activity associated with this replication stream.
     * If method is invoked from different thread of control, it will wait
     * until the rep node thread exits. If it's from the same thread, it's the
     * caller's responsibility to exit the thread upon return from this method.
     *
     * @throws InterruptedException
     * @throws DatabaseException
     */
    public void shutdown()
        throws InterruptedException, DatabaseException {

        if (shutdownDone()) {
            return;
        }

        LoggerUtils.info(logger, repImpl, "Shutting down node " + nameIdPair);

        /* Fire a LeaveGroup if this RepNode is valid. */
        if (repImpl.isValid()) {
            monitorEventManager.notifyLeaveGroup(getLeaveReason());
        }

        /* Stop accepting any new network requests. */
        serviceDispatcher.preShutdown();

        if (elections != null) {
            elections.shutdown();
        }

        /* Initiate the FeederManger soft shutdown if it's active. */
        feederManager.shutdownQueue();

        if ((getReplicaCloseCatchupMs() >= 0) &&
            (nodeState.getRepEnvState().isMaster())) {

            /*
             * A group shutdown. Shutting down the queue will cause the
             * FeederManager to shutdown it's feeders and exit.
             */
            this.join();
        }

        /* Shutdown the replica, if it's active. */
        replica.shutdown();

        shutdownThread(logger);

        LoggerUtils.info(logger, repImpl,
                         "RepNode main thread: " + this.getName() + " exited.");
        /* Shut down all other services. */
        utilityServicesShutdown();

        /* Shutdown all the services before shutting down the dispatcher. */
        MasterTransfer mt = getActiveTransfer();
        if (mt != null) {
            Exception ex = getSavedShutdownException();
            if (ex == null) {
                ex = new MasterTransferFailureException("shutting down");
            }
            mt.abort(ex);
        }
        serviceDispatcher.shutdown();
        LoggerUtils.info(logger, repImpl,
                         nameIdPair + " shutdown completed.");
        masterStatus.setGroupMaster(null, NameIdPair.NULL);
        readyLatch.releaseAwait(getSavedShutdownException());

        /* Cancel the TimerTasks. */
        channelTimeoutTask.cancel();
        if (logFlusher != null) {
            logFlusher.cancelTask();
        }
        timer.cancel();
    }


    /**
     * Soft shutdown for the RepNode thread. Note that since the thread is
     * shared by the FeederManager and the Replica, the FeederManager or
     * Replica specific soft shutdown actions should already have been done
     * earlier.
     */
    @Override
    protected int initiateSoftShutdown() {
        return getThreadWaitInterval();
    }

    /* Get the shut down reason for this node. */
    private LeaveReason getLeaveReason() {
        LeaveReason reason = null;

        Exception exception = getSavedShutdownException();
        if (exception == null) {
            reason = LeaveReason.NORMAL_SHUTDOWN;
        } else if (exception instanceof GroupShutdownException) {
            reason = LeaveReason.MASTER_SHUTDOWN_GROUP;
        } else {
            reason = LeaveReason.ABNORMAL_TERMINATION;
        }

        return reason;
    }

    private void utilityServicesShutdown() {
        if (ldiff != null) {
            ldiff.shutdown();
        }

        if (logFeederManager != null) {
            logFeederManager.shutdown();
        }

        if (binaryNodeStateService != null) {
            binaryNodeStateService.shutdown();
        }

        if (nodeStateService != null) {
            serviceDispatcher.cancel(NodeStateService.SERVICE_NAME);
        }
    }

    /**
     * Must be invoked on the Master via the last open handle.
     *
     * Note that the method itself does not shutdown the group. It merely
     * sets replicaCloseCatchupMs, indicating that the ensuing handle close
     * should shutdown the Replicas. The actual coordination with the closing
     * of the handle is implemented by ReplicatedEnvironment.shutdownGroup().
     *
     * @see ReplicatedEnvironment#shutdownGroup(long, TimeUnit)
     */
    public void shutdownGroupOnClose(long timeoutMs)
        throws IllegalStateException {

        if (!nodeState.getRepEnvState().isMaster()) {
            throw new IllegalStateException
                ("Node state must be " + MASTER +
                 ", not " + nodeState.getRepEnvState());
        }
        replicaCloseCatchupMs = (timeoutMs < 0) ? 0 : timeoutMs;
    }

    /**
     * JoinGroup ensures that a RepNode is actively participating in a
     * replication group. It's invoked each time a replicated environment
     * handle is created.
     *
     * If the node is already participating in a replication group, because
     * it's not the first handle to the environment, it will return without
     * having to wait. Otherwise it will wait until a master is elected and
     * this node is active, either as a Master, or as a Replica.
     *
     * If the node joins as a replica, it will wait further until it has become
     * sufficiently consistent as defined by its consistency argument. By
     * default it uses PointConsistencyPolicy to ensure that it is at least as
     * consistent as the master as of the time the handle was opened.
     *
     * A node can also join in the Unknown state if it has been configured to
     * do so via ENV_UNKNOWN_STATE_TIMEOUT.
     *
     * @throws UnknownMasterException If a master cannot be established within
     * ENV_SETUP_TIMEOUT, unless ENV_UNKNOWN_STATE_TIMEOUT has
     * been set to allow the creation of a handle while in the UNKNOWN state.
     *
     * @return MASTER, REPLICA, or UNKNOWN (if ENV_UNKNOWN_STATE_TIMEOUT
     * is set)
     */
    public ReplicatedEnvironment.State
        joinGroup(ReplicaConsistencyPolicy consistency,
                  QuorumPolicy initialElectionPolicy)
        throws ReplicaConsistencyException, DatabaseException, IOException {

        final JoinGroupTimeouts timeouts =
                new JoinGroupTimeouts(getConfigManager());

        startup(initialElectionPolicy);
        LoggerUtils.finest(logger, repImpl, "joinGroup " +
                           nodeState.getRepEnvState());

        DatabaseException exitException = null;
        int retries=0;
        repImpl.getStartupTracker().start(Phase.BECOME_CONSISTENT);
        repImpl.getStartupTracker().setProgress
           (RecoveryProgress.BECOME_CONSISTENT);
        try {
            for (retries=0; retries < JOIN_RETRIES; retries++ ) {
                try {
                    /* Wait for Feeder/Replica to be fully initialized. */
                    boolean done = getReadyLatch().awaitOrException
                        (timeouts.getTimeout(), TimeUnit.MILLISECONDS);

                    /*
                     * Save the state, and use it from this point forward,
                     * since the node's state may change again.
                     */
                    final ReplicatedEnvironment.State finalState =
                        nodeState.getRepEnvState();
                    if (!done) {
                        /* An election or setup, timeout. */
                        if (finalState.isReplica()) {
                            if (timeouts.timeoutIsForUnknownState()) {
                                /*
                                 * Replica syncing up; move onwards to the
                                 * setup timeout and continue with the syncup.
                                 */
                                timeouts.setSetupTimeout();
                                continue;
                            }
                            throw new ReplicaConsistencyException
                                (String.format("Setup time exceeded %,d ms",
                                               timeouts.getSetupTimeout()),
                                               null);
                        }

                        if (finalState.isUnknown() &&
                            timeouts.timeoutIsForUnknownState()) {
                            return UNKNOWN;
                        }
                        break;
                    }

                    switch (finalState) {
                        case UNKNOWN:

                            /*
                             * State flipped between release of ready latch and
                             * nodeState.getRepEnvState() above; retry for a
                             * Master/Replica state.
                             */
                            continue;

                        case REPLICA:
                            joinAsReplica(consistency);
                            break;

                        case MASTER:
                            LoggerUtils.info(logger, repImpl,
                                             "Joining group as master");
                            break;

                        case DETACHED:
                            throw EnvironmentFailureException.
                                unexpectedState("Node in DETACHED state " +
                                		"while joining group.");
                    }

                    return finalState;
                } catch (InterruptedException e) {
                    throw EnvironmentFailureException.unexpectedException(e);
                } catch (MasterStateException e) {
                    /* Transition to master while establishing consistency. */
                    LoggerUtils.warning(logger, repImpl,
                                        "Join retry due to master transition: "
                                        + e.getMessage());
                    continue;
                } catch (RestartRequiredException e) {
                    LoggerUtils.warning(logger, repImpl,
                                        "Environment needs to be restarted: " +
                                        e.getMessage());
                    throw e;
                } catch (DatabaseException e) {
                    Throwable cause = e.getCause();
                    if ((cause != null) &&
                        (cause.getClass() ==
                         Replica.ConnectRetryException.class)) {

                        /*
                         * The master may have changed. Retry if there is time
                         * left to do so. It may result in a new master.
                         */
                        exitException = e;
                        if (timeouts.getTimeout() > 0) {
                            LoggerUtils.warning(logger, repImpl,
                                                "Join retry due to exception: "
                                                + cause.getMessage());
                            continue;
                        }
                    }
                    throw e;
                }
            }
        } finally {
            repImpl.getStartupTracker().stop(Phase.BECOME_CONSISTENT);
        }

        /* Timed out or exceeded retries. */
        if (exitException != null) {
            LoggerUtils.warning(logger, repImpl, "Exiting joinGroup after " +
                                retries + " retries." + exitException);
            throw exitException;
        }
        throw new UnknownMasterException(null, repImpl.getStateChangeEvent());
    }

    /**
     * Join the group as a Replica ensuring that the node is sufficiently
     * consistent as defined by its consistency policy.
     *
     * @param consistency the consistency policy to use when joining initially
     */
    private void joinAsReplica(ReplicaConsistencyPolicy consistency)
        throws InterruptedException {

        if (consistency == null) {
            final int consistencyTimeout =
                getConfigManager().getDuration(ENV_CONSISTENCY_TIMEOUT);
            consistency = new PointConsistencyPolicy
                (new VLSN(replica.getMasterTxnEndVLSN()),
                 consistencyTimeout, TimeUnit.MILLISECONDS);
        }

        /*
         * Wait for the replica to become sufficiently consistent.
         */
        consistency.ensureConsistency(repImpl);

        /*
         * Flush changes to the file system. The flush ensures in particular
         * that any member database updates defining this node itself are not
         * lost in case of a process crash. See SR 20607.
         */
        repImpl.getLogManager().flushNoSync();

        LoggerUtils.info(logger, repImpl, "Joined group as a replica. " +
                         " join consistencyPolicy=" + consistency +
                         " " + repImpl.getVLSNIndex().getRange());
    }

    /**
     * Should be called whenever a new VLSN is associated with a log entry
     * suitable for Replica/Feeder syncup.
     */
    public void trackSyncableVLSN(VLSN syncableVLSN, long lsn) {
        cbvlsnTracker.track(syncableVLSN, lsn);
    }

    /** May return NULL_VLSN */
    public VLSN getGroupCBVLSN() {
        return globalCBVLSN.getCBVLSN();
    }

    /**
     * Returns the number of nodes needed to form a quorum for elections
     *
     * @param quorumPolicy
     * @return the number of nodes required for a quorum
     */
    public int getElectionQuorumSize(QuorumPolicy quorumPolicy) {
        if (electableGroupSizeOverride > 0) {
            return quorumPolicy.quorumSize(electableGroupSizeOverride);
        }

        if (activePrimary &&
            QuorumPolicy.SIMPLE_MAJORITY.equals(quorumPolicy)) {
            return 1;
        }

        return quorumPolicy.quorumSize(group.getElectableGroupSize());
    }

    /**
     * Returns the minimum number of replication nodes required to
     * implement the ReplicaAckPolicy for a given group size.
     *
     * @return the number of nodes that are needed
     */
    public int minAckNodes(ReplicaAckPolicy ackPolicy) {
        if (electableGroupSizeOverride > 0) {
            return ackPolicy.minAckNodes(electableGroupSizeOverride);
        }

        if (activePrimary && ReplicaAckPolicy.SIMPLE_MAJORITY.
            equals(ackPolicy)) {
            return 1;
        }

        return ackPolicy.minAckNodes(group.getElectableGroupSize());
    }

    /* Convenience overloading */
    public int minAckNodes(Durability durability) {
        return minAckNodes(durability.getReplicaAck());
    }

    /**
     * Returns the group wide CBVLSN. The group CBVLSN is computed as the
     * minimum of CBVLSNs after discarding CBVLSNs that are obsolete. A CBVLSN
     * is considered obsolete, if it has not been updated within a configurable
     * time interval relative to the time that the most recent CBVLSN was
     * updated.
     *
     * @throws DatabaseException
     */
    public void syncupStarted() {
        globalCBVLSN.syncupStarted();
    }

    /*
     * The globalCBVLSN can't be changed when a syncup is in progress. A feeder
     * may have multiple syncups in action.
     */
    public void syncupEnded() {
        globalCBVLSN.syncupEnded();
    }

    /**
     * Returns the file number that forms a barrier for the cleaner's file
     * deletion activities. Files with numbers >= this file number cannot be
     * by the cleaner without disrupting the replication stream.
     *
     * @return the file number that's the barrier for cleaner file deletion
     *
     * @throws DatabaseException
     */
    public long getCleanerBarrierFile()
        throws DatabaseException {

        /* Take the minimum of SyncCleanerBarrier and GlobalCBVLSN. */
        long syncStart = repImpl.getSyncCleanerBarrier().getMinSyncStart();
        if (syncStart != LogChangeSet.NULL_POSITION) {
            VLSN vlsn = new VLSN(syncStart);
            if (vlsn.compareTo(globalCBVLSN.getCBVLSN()) < 0) {
                return repImpl.getVLSNIndex().getLTEFileNumber(vlsn);
            }
        }

        return globalCBVLSN.getCleanerBarrierFile();
    }

    long getReplicaCloseCatchupMs() {
        return replicaCloseCatchupMs;
    }

    /**
     * Returns true if the node is a designated Primary that has been
     * activated.
     */
    public boolean isActivePrimary() {
        return activePrimary;
    }

    /**
     * Tries to activate this node as a Primary, if it has been configured as
     * such and if the group size is two. This method is invoked when an
     * operation falls short of quorum requirements and is ready to trade
     * durability for availability. More specifically it's invoked when an
     * election fails, or there is an insufficient number of replicas during
     * a begin transaction or a transaction commit.
     *
     * The Primary is passivated again when the Secondary contacts it.
     *
     * @return true if the primary was activated -- the quorum value is 1
     */
    public boolean tryActivatePrimary() {
        if (checkDesignatedPrimary()) {
            LoggerUtils.info(logger, repImpl,
                             "Primary activated; quorum is one.");
            activePrimary = true;
            return true;
        }
        return false;
    }

    /**
     * Checks whether we're in a 2-node group and configured as the Designated
     * Primary.
     */
    public boolean checkDesignatedPrimary() {
        return (repImpl != null) && /* Not a dummy test rep node. */
            repImpl.isDesignatedPrimary() &&
            getGroup().getElectableGroupSize() == 2;
    }

    /*
     * Invoked whenever there is an opportunity to passivate a node. Typically
     * when this node is contacted by another node and the other node is
     * sufficiently current so as to be able to respond to ack requests. Or
     * when the node is no longer the designated primary.
     */
    final public void passivatePrimary() {
        if (activePrimary) {
            LoggerUtils.info(logger, repImpl, "Primary passivated.");
        }
        activePrimary = false;
    }

    /**
     * Shuts down the Network backup service *before* a rollback is initiated
     * as part of syncup, thus ensuring that NetworkRestore does not see an
     * inconsistent set of log files. Any network backup operations that are in
     * progress at this node are aborted. The client of the service will
     * experience network connection failures and will retry with this node
     * (when the service is re-established at this node), or with some other
     * node.
     * <p>
     * restarNetworkBackup() is then used to restart the service after it was
     * shut down.
     */
    final public void shutdownNetworkBackup() {
        logFeederManager.shutdown();
        logFeederManager = null;
    }

    /**
     * Restarts the network backup service *after* a rollback has been
     * completed and the log files are once again in a consistent state.
     */
    final public void restartNetworkBackup() {
        if (logFeederManager != null) {
            throw EnvironmentFailureException.unexpectedState(repImpl);
        }
        logFeederManager=
            new com.sleepycat.je.rep.impl.networkRestore.FeederManager
            (serviceDispatcher, repImpl, nameIdPair);
    }

    /*
     * Used to create deliberate clock skews for testing purposes. Replicator
     * code should use it instead of invoking System.currentTimeMillis()
     * directly.
     */
    public static class Clock {
        private final int skewMs;

        private Clock(int skewMs) {
            this.skewMs = skewMs;
        }

        public long currentTimeMillis() {
            return System.currentTimeMillis() + skewMs;
        }
    }

    /**
     * Dumps the states associated with any active Feeders as well as
     * information pertaining to the group CBVLSN and the composition of the
     * group itself.
     */
    public String dumpState() {
        return  "\n" + feederManager.dumpState() +
            "\nGlobalCBVLSN=" + getGroupCBVLSN() +
            "\n" + getGroup();
    }

    /**
     * Dumps the state associated with all active Feeders.
     */
    public String dumpFeederState() {
        return  "\n" + feederManager.dumpState() + "\n";
    }

    /*
     * Sets the override value for the Electable Group size.
     */
    public void setElectableGroupSizeOverride(int override) {
        if (electableGroupSizeOverride != override) {
            LoggerUtils.warning(logger, repImpl,
                                "Electable group size override changed to:" +
                                override);
        }
        this.electableGroupSizeOverride = override;
    }
}
