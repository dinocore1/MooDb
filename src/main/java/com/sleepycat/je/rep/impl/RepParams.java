/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.StringTokenizer;

import com.sleepycat.je.config.BooleanConfigParam;
import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.DurationConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.config.IntConfigParam;
import com.sleepycat.je.config.LongConfigParam;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.util.DbResetRepGroup;
import com.sleepycat.je.rep.utilint.RepUtils;

public class RepParams {

    /*
     * Note: all replicated parameters should start with
     * EnvironmentParams.REP_PARAM_PREFIX, which is "je.rep.",
     * see SR [#19080].
     */

    /**
     * @hidden
     * Name of a java System property (boolean) which can be turned on in order
     * to avoid input validation checks on node names.  This is undocumented.
     * <p>
     * Generally users should not skip validation, because there are a few
     * kinds of punctuation characters that would cause problems if they were
     * allowed in node names.  But in the past users might have inadvertantly
     * created node names that do not conform to the new, stricter rules.  In
     * that case they would not be able to upgrade to the newer version of JE
     * that now includes this checking.
     * <p>
     * This flag actually applies to the group name too.  But for group names
     * the new rules are actually less strict than they used to be, so there
     * should be no problem.
     */
    public static final String SKIP_NODENAME_VALIDATION =
        "je.rep.skipNodenameValidation";

    /**
     * A JE/HA configuration parameter describing an Identifier name.
     */
    static public class IdentifierConfigParam extends ConfigParam {
        private static final String DEBUG_NAME =
            IdentifierConfigParam.class.getName();

        public IdentifierConfigParam(String configName,
                                     String defaultValue,
                                     boolean mutable,
                                     boolean forReplication) {
            super(configName, defaultValue, mutable, forReplication);
        }

        @Override
        public void validateValue(String value) {
            if (Boolean.getBoolean(SKIP_NODENAME_VALIDATION)) {
                return;
            }
            if ((value == null) || (value.length() == 0)) {
                throw new IllegalArgumentException
                    (DEBUG_NAME + ": a value is required");
            }
            for (char c : value.toCharArray()) {
                if (!isValid(c)) {
                    throw new IllegalArgumentException
                        (DEBUG_NAME + ": " + name + ", must consist of " +
                         "letters, digits, hyphen, underscore, period.");
                }
            }
        }

        private boolean isValid(char c) {
            if (Character.isLetterOrDigit(c) ||
                c == '-' ||
                c == '_' ||
                c == '.') {
                return true;
            }
            return false;
        }
    }

    /*
     * Replication group-wide properties. These properties are candidates for
     * consistency checking whenever there is a handshake between a master and
     * replica.
     */

    /** Names the Replication group. */
    public static final ConfigParam GROUP_NAME =
        new IdentifierConfigParam(ReplicationConfig.GROUP_NAME,
                                  "DefaultGroup",      // default
                                  false,               // mutable
                                  true);               // forReplication

    /**
     * The maximum amount of time the replication group guarantees preservation
     * of the log files constituting the replication stream. After this period
     * of time, nodes are free to do log cleaning and to remove log files
     * earlier than this period. If a node has crashed and does not re-join the
     * group within this timeout period it may need to perform a network
     * restore operation to catch up.
     */
    public static final DurationConfigParam REP_STREAM_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.REP_STREAM_TIMEOUT,
                                null,                         // min
                                null,                         // max
                                "24 h",                       // default
                                false,                        // mutable
                                true);

    /**
     * @see ReplicationConfig#REPLICA_RECEIVE_BUFFER_SIZE
     */
    public static final IntConfigParam REPLICA_RECEIVE_BUFFER_SIZE =
            new IntConfigParam(ReplicationConfig.REPLICA_RECEIVE_BUFFER_SIZE,
                               0,               // min
                               null,            // max
                               1048576,         // default
                               false,           // mutable
                               true);           // forReplication

    /**
     * The size of the message queue used for communication between the thread
     * reading the replication stream and the thread doing the replay. The
     * default buffer size has been chosen to hold 500 single operation
     * transactions (the ln + commit record) assuming 1K sized LN record.
     * <p>
     * Larger values of buffer size may result in higher peak memory
     * utilization, due to a larger number of LNs sitting in the queue. The
     * size of the queue itself is unlikely to be an issue, since it's tiny
     * relative to cache sizes. At 1000, 1kbyte LNs it raises the peak
     * utilization by 1MB which for most apps is an insignificant rise in the
     * peak.
     *
     * Note that the parameter is lazily mutable, that is, the change will take
     * effect the next time the node transitions to a replica state.
     */
    public static final IntConfigParam REPLICA_MESSAGE_QUEUE_SIZE =
            new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "replicaMessageQueueSize",
                               1,               // min
                               null,            // max
                               1000,            // default
                               true,            // mutable
                               true);           // forReplication

    /**
     * The lock timeout for replay transactions.
     */
    public static final DurationConfigParam REPLAY_TXN_LOCK_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.REPLAY_TXN_LOCK_TIMEOUT,
                                "1 ms",            // min
                                "75 min",          // max
                                "500 ms",          // default
                                false,             // mutable
                                true);             // forReplication

    /**
     * @see ReplicationConfig#ENV_SETUP_TIMEOUT
     */
    public static final DurationConfigParam ENV_SETUP_TIMEOUT =
        new DurationConfigParam
        (ReplicationConfig.ENV_SETUP_TIMEOUT,
         null,                          // min
         null,                          // max
         "10 h",                        // default 10 hrs
         false,                         // mutable
         true);

    /**
     * @see ReplicationConfig#ENV_CONSISTENCY_TIMEOUT
     */
    public static final DurationConfigParam
        ENV_CONSISTENCY_TIMEOUT =
            new DurationConfigParam(ReplicationConfig.ENV_CONSISTENCY_TIMEOUT,
                                    "10 ms",                      // min
                                    null,                         // max
                                    "5 min",                      // default
                                    false,                        // mutable
                                    true);

    /**
     * @see ReplicationConfig#ENV_UNKNOWN_STATE_TIMEOUT
     */
    public static final DurationConfigParam ENV_UNKNOWN_STATE_TIMEOUT =
        new DurationConfigParam
        (ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT,
         null,                          // min
         null,                          // max
         "0 s",                         // default
         false,                         // mutable
         true);

    /**
     * @see ReplicationConfig#REPLICA_ACK_TIMEOUT
     */
    public static final DurationConfigParam REPLICA_ACK_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.REPLICA_ACK_TIMEOUT,
                                "10 ms",                     // min
                                null,                        // max
                                "5 s",                       // default
                                false,                       // mutable
                                true);                       // forReplication

    /**
     * @see ReplicationConfig#INSUFFICIENT_REPLICAS_TIMEOUT
     */
    public static final DurationConfigParam INSUFFICIENT_REPLICAS_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.INSUFFICIENT_REPLICAS_TIMEOUT,
                                "10 ms",                     // min
                                null,                        // max
                                "10 s",                       // default
                                false,                       // mutable
                                true);                       // forReplication

    /**
     * The maximum message size which will be accepted by a node (to prevent
     * DOS attacks).  While the default shown here is 0, it dynamically
     * calculated when the node is created and is set to the half of the
     * environment cache size. The cache size is mutable, but changing the
     * cache size at run time (after environment initialization) will not
     * change the value of this parameter.  If a value other than cache size /
     * 2 is desired, this non-mutable parameter should be specified at
     * initialization time.
     */
    public static final LongConfigParam MAX_MESSAGE_SIZE =
        new LongConfigParam(ReplicationConfig.MAX_MESSAGE_SIZE,
                            Long.valueOf(1 << 18),        // min (256KB)
                            Long.valueOf(Long.MAX_VALUE), // max
                            Long.valueOf(0),         // default (cachesize / 2)
                            false,                   // mutable
                            true);                   // forReplication

    /**
     * Identifies the default consistency policy used by a replica. Only two
     * policies are meaningful as properties denoting environment level default
     * policies: NoConsistencyRequiredPolicy and TimeConsistencyPolicy.  They
     * can be specified as: NoConsistencyRequiredPolicy or
     * TimeConsistencyPolicy(<permissibleLag>,<timeout>). For example, a time
     * based consistency policy with a lag of 1 second and a timeout of 1 hour
     * is denoted by the string: TimeConsistencyPolicy(1000,3600000)
     */
    public static final ConfigParam CONSISTENCY_POLICY =
        new ConfigParam(ReplicationConfig.CONSISTENCY_POLICY,
                        // Default lag of 1 sec, and timeout of 1 hour
                        "TimeConsistencyPolicy(1 s,1 h)",
                        false,                   // mutable
                        true) {                  // for Replication
        @Override
        public void validateValue(String propertyValue)
            throws IllegalArgumentException {

            /* Evaluate for the checking side-effect. */
            RepUtils.getReplicaConsistencyPolicy(propertyValue);
        }
    };

    /* The ports used by a replication group */

    /**
     * The port used for replication.
     */
    public static final IntConfigParam DEFAULT_PORT =
        new IntConfigParam(ReplicationConfig.DEFAULT_PORT,
                           Integer.valueOf(1024),   // min
                           Integer.valueOf(Short.MAX_VALUE), // max
                           Integer.valueOf(5001),   // default
                           false,                   // mutable
                           true);                   // forReplication

    /**
     * Names the host (or interface) and port associated with the node in the
     * replication group, e.g. je.rep.nodeHostPort=foo.com:5001
     */
    public static final ConfigParam NODE_HOST_PORT =
        new ConfigParam(ReplicationConfig.NODE_HOST_PORT,
                        "localhost",         // default
                        false,               // mutable
                        true) {              // forReplication

        @Override
        public void validateValue(String hostAndPort)
            throws IllegalArgumentException {

            if ((hostAndPort == null) || (hostAndPort.length() == 0)) {
                throw new IllegalArgumentException
                    ("The value cannot be null or zero length: " + name);
            }
            int colonToken = hostAndPort.indexOf(":");
            String hostName = (colonToken >= 0) ?
                               hostAndPort.substring(0,colonToken) :
                               hostAndPort;
            ServerSocket testSocket = null;
            try {
                testSocket = new ServerSocket();
                /* The bind will fail if the hostName does not name this m/c.*/
                testSocket.bind(new InetSocketAddress(hostName, 0));
                testSocket.close();
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException
                    ("Property: " + name +
                     " Invalid hostname: " + hostName, e);
            } catch (IOException e) {

                /*
                 * Server socket could not be bound to any port. Hostname is
                 * not associated with this m/c.
                 */
                throw new IllegalArgumentException
                    ("Property: " + name +
                     " Invalid hostname: " + hostName, e);
            }

            if (colonToken >= 0) {
                validatePort(hostAndPort.substring(colonToken+1));
            }
        }
    };

    /*
     * The Name uniquely identifies this node within the replication group.
     */
    public static final ConfigParam NODE_NAME =
        new IdentifierConfigParam(ReplicationConfig.NODE_NAME,
                                  "DefaultRepNodeName",// default
                                  false,               // mutable
                                  true);               // forReplication

    /*
     * Identifies the type of the node.
     */
    public static final EnumConfigParam<NodeType> NODE_TYPE =
        new EnumConfigParam<NodeType>(ReplicationConfig.NODE_TYPE,
                                      NodeType.ELECTABLE,         // default
                                      false,                      // mutable
                                      true,
                                      NodeType.class);

    /*
     * Associated a priority with this node. The priority is used during
     * elections to favor one node over another. All other considerations being
     * equal, the priority is used as a tie-breaker; the node with the higher
     * priority is selected as the master.
     */
    public static final IntConfigParam NODE_PRIORITY =
        new IntConfigParam(ReplicationMutableConfig.NODE_PRIORITY,
                           Integer.valueOf(0),   // min
                           Integer.valueOf(Integer.MAX_VALUE), // max
                           Integer.valueOf(1),   // default
                           true,                 // mutable
                           true);                // forReplication

    /*
     * Identifies the Primary node in a two node group.
     */
    public static final BooleanConfigParam DESIGNATED_PRIMARY  =
        new BooleanConfigParam(ReplicationMutableConfig.DESIGNATED_PRIMARY,
                               false,           // default
                               true,            // mutable
                               true);


    /*
     * An internal option used to control the use of Nagle's algorithm
     * on feeder connections. A value of true disables use of Nagle's algorithm
     * and causes output to be sent immediately without delay.
     */
    public static final BooleanConfigParam FEEDER_TCP_NO_DELAY  =
            new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                                   "feederTcpNoDelay",
                                   true,              // default
                                   false,            // mutable
                                   true);

    /**
     * @see ReplicationMutableConfig#ELECTABLE_GROUP_SIZE_OVERRIDE
     */
    public static final IntConfigParam ELECTABLE_GROUP_SIZE_OVERRIDE =
        new IntConfigParam(ReplicationMutableConfig.
                           ELECTABLE_GROUP_SIZE_OVERRIDE,
                           Integer.valueOf(0),                // min
                           Integer.valueOf(Integer.MAX_VALUE),// max
                           Integer.valueOf(0),                // default
                           true,                              // mutable
                           true);                             // forReplication

    /**
     * An internal option, accessed only via the utility
     * {@link DbResetRepGroup} utility, to reset a replication group to a
     * single new member when the replicated environment is opened.
     */
    public static final BooleanConfigParam RESET_REP_GROUP  =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "resetRepGroup",
                               false,            // default
                               false,            // mutable
                               true);
    /*
     * Sets the maximum allowable skew between a Feeder and its replica. The
     * clock skew is checked as part of the handshake when the Replica
     * establishes a connection to its Feeder.
     */
    public static final DurationConfigParam MAX_CLOCK_DELTA =
        new DurationConfigParam(ReplicationConfig.MAX_CLOCK_DELTA,
                                null,               // min
                                "1 min",            // max
                                "2 s",              // default
                                false,              // mutable
                                true);              // forReplication

    /*
     * The list of helper node and port pairs.
     */
    public static final ConfigParam HELPER_HOSTS =
        new ConfigParam(ReplicationConfig.HELPER_HOSTS,
                        "",                  // default
                        false,               // mutable
                        true) {              // forReplication

        @Override
        public void validateValue(String hostPortPairs)
            throws IllegalArgumentException {

            if ((hostPortPairs == null) || (hostPortPairs.length() == 0)) {
                return;
            }
            HashSet<String> hostPortSet = new HashSet<String>();
            for (StringTokenizer tokenizer =
                 new StringTokenizer(hostPortPairs,",");
                 tokenizer.hasMoreTokens();) {
                try {
                    String hostPortPair = tokenizer.nextToken();
                    if (!hostPortSet.add(hostPortPair)) {
                        throw new IllegalArgumentException
                            ("Property: " + name +
                             " Duplicate specification: " + hostPortPair);
                    }
                    validateHostAndPort(hostPortPair);
                } catch (IllegalArgumentException iae) {
                    throw new IllegalArgumentException
                        ("Property: " + name + "Error: " + iae.getMessage(),
                         iae);
                }
            }
        }
    };

    /* Heartbeat interval in milliseconds. */
    public static final IntConfigParam HEARTBEAT_INTERVAL =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "heartbeatInterval",
         Integer.valueOf(1000),// min
         null,                 // max
         Integer.valueOf(1000),// default
         false,                // mutable
         true);                // forReplication

    /* Replay Op Count after which we clear the DbTree cache. */
    public static final IntConfigParam DBTREE_CACHE_CLEAR_COUNT =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "dbIdCacheOpCount",
         Integer.valueOf(1),    // min
         null,                  // max
         Integer.valueOf(5000), // default
         false,                 // mutable
         true);                 // forReplication

    /*
     * The default value is chosen to retain at least one log file so that in
     * the event of a Replica crash when using no_sync, the replica can resume
     * processing the replication stream without requiring a network restore.
     * So the default roughly corresponds to a 50M log file, with 1000 byte LN
     * entries.
     *
     * Note that there is a relationship between this parameter and
     * CLEANER_MIN_AGE, which together set the bounds for the cleaner.
     */
    public static final IntConfigParam CBVLSN_PAD =
        new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX + "cbvlsn.pad",
                           Integer.valueOf(0),    // min
                           null,                  // max
                           Integer.valueOf(50000),// default
                           false,                 // mutable
                           true);                 // forReplication

    public static final IntConfigParam VLSN_STRIDE =
        new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX + "vlsn.stride",
                           Integer.valueOf(1),     // min
                           null,                   // max
                           Integer.valueOf(10),    // default
                           false,                  // mutable
                           true);                  // forReplication

    public static final IntConfigParam VLSN_MAX_MAP =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "vlsn.mappings",
         Integer.valueOf(1),      // min
         null,                    // max
         Integer.valueOf(1000),   // default
         false,                   // mutable
         true);                   // forReplication

    public static final IntConfigParam VLSN_MAX_DIST =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "vlsn.distance",
         Integer.valueOf(1),      // min
         null,                    // max
         Integer.valueOf(100000), // default
         false,                   // mutable
         true);                   // forReplication

    /*
     * Internal testing use only: Simulate a delay in the replica loop for test
     * purposes. The value is the delay in milliseconds.
     */
    public static final IntConfigParam TEST_REPLICA_DELAY =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "test.replicaDelay",
         Integer.valueOf(0), // min
         Integer.valueOf(Integer.MAX_VALUE), // max
         Integer.valueOf(0), // default
         false,              // mutable
         true);              // forReplication

    /*
     * Sets the VLSNIndex cache holding recent log items in support of the
     * feeders. The size must be a power of two.
     */
    public static final IntConfigParam VLSN_LOG_CACHE_SIZE =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "vlsn.logCacheSize",
         Integer.valueOf(0),      // min
         Integer.valueOf(1<<10),  // max
         Integer.valueOf(32),     // default
         false,                   // mutable
         true);                   // forReplication

    /*
     * The socket timeout value used by a Replica when it opens a new
     * connection to establish a replication stream with a feeder.
     */
    public static final DurationConfigParam REPSTREAM_OPEN_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "repstreamOpenTimeout",
         null,               // min
         "5 min",            // max
         "5 s",              // default
         false,              // mutable
         true);              // forReplication

    /*
     * The socket timeout value used by Elections agents when they open
     * sockets to communicate with each other using the Elections protocol.
     */
    public static final DurationConfigParam ELECTIONS_OPEN_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "electionsOpenTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /*
     * The maximum amount of time a Learner or Acceptor agent will wait for
     * input on a network connection, while listening for a message before
     * timing out. This timeout applies to the Elections protocol.
     */
    public static final DurationConfigParam ELECTIONS_READ_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "electionsReadTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /**
     * The master re-broadcasts the results of an election with this period.
     */
    public static final DurationConfigParam
        ELECTIONS_REBROADCAST_PERIOD =
        new DurationConfigParam
        (ReplicationConfig.ELECTIONS_REBROADCAST_PERIOD,
         null,               // min
         null,               // max
         "1 min",            // default
         false,              // mutable
         true);

    /**
     * @see ReplicationConfig#ELECTIONS_PRIMARY_RETRIES
     */
    public static final IntConfigParam ELECTIONS_PRIMARY_RETRIES =
        new IntConfigParam(ReplicationConfig.ELECTIONS_PRIMARY_RETRIES,
                           0,
                           Integer.MAX_VALUE,
                           2,
                           false,
                           true);

    /*
     * Socket open timeout for use with the RepGroupProtocol.
     */
    public static final DurationConfigParam REP_GROUP_OPEN_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "repGroupOpenTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /*
     * Socket read timeout for use with the RepGroupProtocol.
     */
    public static final DurationConfigParam REP_GROUP_READ_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "repGroupReadTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /*
     * Socket open timeout for use with the Monitor Protocol.
     */
    public static final DurationConfigParam MONITOR_OPEN_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "monitorOpenTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /*
     * Socket read timeout for use with the MonitorProtocol.
     */
    public static final DurationConfigParam MONITOR_READ_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "monitorReadTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /**
     * @see ReplicationConfig#REPLICA_TIMEOUT
     */
    public static final  DurationConfigParam REPLICA_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.REPLICA_TIMEOUT,
                           "2 s", // min
                           null, // max
                           "30 s", // default
                           false,              // mutable
                           true);              // forReplication

    /* @see ReplicationConfig#REPLAY_MAX_OPEN_DB_HANDLES */
    public static final IntConfigParam REPLAY_MAX_OPEN_DB_HANDLES =
        new IntConfigParam(ReplicationMutableConfig.REPLAY_MAX_OPEN_DB_HANDLES,
                           Integer.valueOf(1), // min
                           Integer.valueOf(Integer.MAX_VALUE), // max
                           Integer.valueOf(10), // default
                           true,               // mutable
                           true);              // forReplication

    /* @see ReplicationConfig#REPLAY_DB_HANDLE_TIMEOUT */
    public static final DurationConfigParam REPLAY_DB_HANDLE_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.REPLAY_DB_HANDLE_TIMEOUT,
                                "1 s",              // min
                                null,               // max
                                "30 s",             // default
                                true,              // mutable
                                true);              // forReplication
    /*
     * The number of heartbeat responses that must be detected as missing
     * during an otherwise idle period before the Feeder shuts down the
     * connection with the Replica.
     *
     * This value provides the basis for the "read timeout" used by the Feeder
     * when communicating with the Replica. The timeout is calculated as
     * FEEDER_HEARTBEAT_TIMEOUT * HEARTBEAT_INTERVAL. Upon a timeout the Feeder
     * closes the connection.
     *
     * Reducing this value permits the master to discover failed Replicas
     * faster. However, it increases the chances of false positives as well, if
     * the network is experiencing transient problems from which it might
     * just recover.
     */
    public static final IntConfigParam FEEDER_HEARTBEAT_TIMEOUT =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "feederHeartbeatTrigger",
         Integer.valueOf(0), // min
         Integer.valueOf(Integer.MAX_VALUE), // max
         Integer.valueOf(4), // default
         false,              // mutable
         true);

    /**
     * @see ReplicationConfig#FEEDER_TIMEOUT
     */
    public static final DurationConfigParam FEEDER_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.FEEDER_TIMEOUT,
                                "2 s", // min
                                null, // max
                                "30 s", // default
                                false,              // mutable
                                true);              // forReplication

    /**
     * Used to log an info message when a commit log record exceeds this
     * time interval from the time it was created, to the time it was written
     * out to the network.
     */
    public static final DurationConfigParam TRANSFER_LOGGING_THRESHOLD =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "transferLoggingThreshold",
         "1 ms",              // min
         null,               // max
         "5 s",              // default
         false,              // mutable
         true);              // forReplication

    /**
     * Used to log an info message when the time taken to replay a single log
     * entry at a replica exceeds this threshold.
     */
    public static final DurationConfigParam REPLAY_LOGGING_THRESHOLD =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "replayLoggingThreshold",
         "1 ms",             // min
         null,               // max
         "5 s",              // default
         false,              // mutable
         true);              // forReplication


    /**
     * Changes the notion of an ack. When set to true, a replica is considered
     * to have acknowledged a commit as soon as the feeder has written the
     * commit record to the network. That is, it does not wait for the replica
     * to actually acknowledge the commit via a return message. This permits
     * the master to operate in a more async manner relative to the replica
     * provide for higher throughput.
     *
     * This config parameter is internal.
     */
    public static final BooleanConfigParam COMMIT_TO_NETWORK =
        new BooleanConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "commitToNetwork",
         false,            // default
         false,             // mutable
         true);


    public static final DurationConfigParam PRE_HEARTBEAT_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "preHeartbeatTimeoutMs",
         "1 s", // min
         null, // max
         "60 s", // default
         false,              // mutable
         true);

    /**
     * Verifies that the port is a reasonable number. The port must be outside
     * the range of "Well Known Ports" (zero through 1024).
     *
     * @param portString the string representing the port.
     */
    private static void validatePort(String portString)
        throws IllegalArgumentException {

        try {
            int port = Integer.parseInt(portString);

            if ((port <= 0) || (port > 0xffff)) {
                throw new IllegalArgumentException
                    ("Invalid port number: " + portString);
            }
            if (port <= 1023) {
                throw new IllegalArgumentException
                    ("Port number " + port +
                     " is invalid because the port must be outside the range of \"well known\" ports");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException
                ("Invalid port number: " + portString);
        }
    }

    /**
     * Validates that the hostPort is a string of the form:
     *
     * hostName[:port]
     *
     * @param hostAndPort
     * @throws IllegalArgumentException
     */
    private static void validateHostAndPort(String hostAndPort)
        throws IllegalArgumentException {

        int colonToken = hostAndPort.indexOf(":");
        String hostName = (colonToken >= 0) ?
            hostAndPort.substring(0,colonToken) :
            hostAndPort;
        if ("".equals(hostName)) {
            throw new IllegalArgumentException("missing hostname");
        }
        try {
            InetAddress.getByName(hostName);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException
                ("Invalid hostname: " + e.getMessage());
        }
        if (colonToken >= 0) {
            validatePort(hostAndPort.substring(colonToken+1));
        }
    }

    /**
     * @see ReplicationConfig#TXN_ROLLBACK_LIMIT
     */
    public static final IntConfigParam TXN_ROLLBACK_LIMIT =
        new IntConfigParam(ReplicationConfig.
                           TXN_ROLLBACK_LIMIT,
                           Integer.valueOf(0),                // min
                           Integer.valueOf(Integer.MAX_VALUE),// max
                           Integer.valueOf(10),               // default
                           false,                             // mutable
                           true);                             // forReplication

    /**
     * @see ReplicationConfig#RUN_LOG_FLUSH_TASK
     */
    public static final BooleanConfigParam RUN_LOG_FLUSH_TASK  =
        new BooleanConfigParam(ReplicationMutableConfig.RUN_LOG_FLUSH_TASK,
                               true,             // default
                               true,             // mutable
                               true);            // forReplication

    /**
     * @see ReplicationConfig#LOG_FLUSH_TASK_INTERVAL
     */
    public static final DurationConfigParam LOG_FLUSH_TASK_INTERVAL =
        new DurationConfigParam
        (ReplicationMutableConfig.LOG_FLUSH_TASK_INTERVAL,
         "1 s",           // min
         null,            // max
         "5 min",         // default
         true,            // mutable
         true);           // forReplication

    /**
     * @see ReplicationConfig#ALLOW_UNKNOWN_STATE_ENV_OPEN
     */
    @SuppressWarnings({ "javadoc", "deprecation" })
    public static final BooleanConfigParam ALLOW_UNKNOWN_STATE_ENV_OPEN =
        new BooleanConfigParam(ReplicationConfig.ALLOW_UNKNOWN_STATE_ENV_OPEN,
                               false,           // default
                               false,           // mutable
                               true);

    /**
     * If true, the replica runs with this property will not join the
     * replication group.
     */
    public static final BooleanConfigParam DONT_JOIN_REP_GROUP =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "dontJoinRepGroup",
                               false,
                               false,
                               true);

    /**
     * Internal parameter to preserve record version (VLSN).  Is immutable
     * forever, i.e., it may not be changed after the environment has been
     * created.  It has the following impacts:
     *
     * . The VLSN is stored with the LN in the Btree and is available via the
     *   CursorImpl API.
     * . The VLSN is included when migrating an LN during log cleaning.
     *
     * FUTURE: Expose this in ReplicationConfig and improve doc if we make
     * record versions part of the public API.
     */
    public static final BooleanConfigParam PRESERVE_RECORD_VERSION =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "preserveRecordVersion",
                               false,         // default
                               false,         // mutable
                               true);         // forReplication

    /**
     * Whether to cache the VLSN in the BIN after the LN has been stripped by
     * eviction, unless caching is explicitly disabled using the
     * CACHE_RECORD_VERSION setting.
     *
     * This setting has no impact if PRESERVE_RECORD_VERSION is not also
     * enabled.
     *
     * FUTURE: Expose this in ReplicationConfig and improve doc if we make
     * record versions part of the public API.
     */
    public static final BooleanConfigParam CACHE_RECORD_VERSION =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "cacheRecordVersion",
                               true,          // default
                               false,         // mutable
                               true);         // forReplication

    /**
     * The initial number of bytes per record version (per VLSN sequence) in
     * the record version cach.  The default value, 5, is appropriate for data
     * set sizes roughly from 1 to 100 billion.  A smaller value may be
     * configured to save memory for smaller data sets.  A larger value may be
     * configured to avoid mutation of the cache as the data set grows.
     *
     * This setting has no impact unless CACHE_RECORD_VERSION and
     * PRESERVE_RECORD_VERSION are not also enabled.
     *
     * FUTURE: Expose this in ReplicationConfig and improve doc if we make
     * record versions part of the public API.
     */
    public static final IntConfigParam CACHED_RECORD_VERSION_MIN_LENGTH =
        new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                           "cachedRecordVersionMinLength",
                           Integer.valueOf(1), // min
                           Integer.valueOf(8), // max
                           Integer.valueOf(5), // default
                           false,              // mutable
                           true);              // forReplication

    /**
     * @see ReplicationConfig#PROTOCOL_OLD_STRING_ENCODING
     * TODO: Change default to false in JE 5.1.
     */
    public static final BooleanConfigParam PROTOCOL_OLD_STRING_ENCODING =
        new BooleanConfigParam(ReplicationConfig.PROTOCOL_OLD_STRING_ENCODING,
                               true,          // default
                               false,         // mutable
                               true);         // forReplication
}
