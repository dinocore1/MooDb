/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.StringTokenizer;

import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.impl.BinaryNodeStateProtocol;
import com.sleepycat.je.rep.impl.BinaryNodeStateProtocol.BinaryNodeStateResponse;
import com.sleepycat.je.rep.impl.BinaryNodeStateService;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.utilint.CmdUtil;

/**
 * This class provides the utility to request the current state of a replica in 
 * a JE replication group, see more details in 
 * {@link com.sleepycat.je.rep.NodeState}.
 */
public class DbPing {
    /* The name of the state requested node. */
    private String nodeName;
    /* The name of group which the requested node joins. */
    private String groupName;
    /* The SocketAddress of the requested node. */
    private InetSocketAddress socketAddress;
    /* The timeout value for building the connection. */
    private int socketTimeout = 10000;

    private static final String usageString = 
        "Usage: " + CmdUtil.getJavaCommand(DbPing.class) + "\n" + 
        "  -nodeName <node name>      # name of the node whose state is\n" +
        "                             # requested\n" + 
        "  -groupName <group name>    # name of the group which the node\n" +
        "                             # joins\n" +
        "  -nodeHost <host:port>      # the host name and port pair the\n" +
        "                             # node used to join the group\n" +
        "  -socketTimeout <optional>  # the timeout value for creating a\n" +
        "                             # socket connection with the node,\n" +
        "                             # default is 10 seconds if not set";

    /**
     * Usage:
     * <pre>
     * java {com.sleepycat.je.rep.util.DbPing |
     *       -jar je-&lt;version&gt;.jar DbPing}
     *   -nodeName &lt;node name&gt; # name of the node whose state is  
     *                               # requested
     *   -groupName &lt;group name&gt; # name of the group which the node joins
     *   -nodeHost &lt;host:port&gt; # the host name and port pair the node
     *                               # used to join the group
     *   -socketTimeout              # the timeout value for creating a
     *                               # socket connection with the node,
     *                               # default is 10 seconds if not set                            
     * </pre>
     */
    public static void main(String args[])
        throws Exception {

        DbPing ping = new DbPing();
        ping.parseArgs(args);
        System.out.println(ping.getNodeState());
    }

    /**
     * Print usage information for this utility.
     *
     * @param message the errors description.
     */
    private void printUsage(String msg) {
        if (msg != null) {
            System.err.println(msg);
        }

        System.err.println(usageString);
        System.exit(-1);
    }

    /**
     * Parse the command line parameters.
     *
     * @param argv Input command line parameters.
     */
    private void parseArgs(String argv[]) {
        int argc = 0;
        int nArgs = argv.length;

        if (nArgs == 0) {
            printUsage(null);
        }

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-nodeName")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                } else {
                    printUsage("-nodeName requires an argument");
                }
            } else if (thisArg.equals("-groupName")) {
                if (argc < nArgs) {
                    groupName = argv[argc++];
                } else {
                    printUsage("-groupName requires an argument");
                }
            } else if (thisArg.equals("-nodeHost")) {
                if (argc < nArgs) {
                    StringTokenizer st = 
                        new StringTokenizer(argv[argc++], ":");
                    if (st.countTokens() != 2) {
                        printUsage("Argument for -nodeHost is not valid.");
                    }
                    try {
                        socketAddress = new InetSocketAddress
                            (st.nextToken(), Integer.parseInt(st.nextToken()));
                    } catch (NumberFormatException e) {
                        printUsage("the port of -nodeHost is not valid");
                    }
                } else {
                    printUsage("-nodeHost requires an argument");
                }
            } else if (thisArg.equals("-socketTimeout")) {
                if (argc < nArgs) {
                    try {
                        socketTimeout = Integer.parseInt(argv[argc++]);
                    } catch (NumberFormatException e) {
                        printUsage("Argument for -socketTimeout is not valid");
                    }
                } else {
                    printUsage("-socketTimeout requires an argument");
                }
            } else {
                printUsage(thisArg + " is not a valid argument");
            }
        }

        if (socketTimeout <= 0) {
            printUsage("-socketTimeout requires a positive integer number");
        }

        if (nodeName == null || groupName == null || socketAddress == null) {
            printUsage("Node name, group name and the node host port are " + 
                       "mandatory arguments, please configure.");
        }
    }

    private DbPing() {
    }

    /**
     * Create a DbPing instance for programmatic use.
     *
     * @param repNode a class that implements 
     * {@link com.sleepycat.je.rep.ReplicationNode}
     * @param groupName name of the group which the node joins
     * @param socketTimeout timeout value for creating a socket connection
     * with the node
     */
    public DbPing(ReplicationNode repNode, 
                  String groupName, 
                  int socketTimeout) {
        this.nodeName = repNode.getName();
        this.groupName = groupName;
        this.socketAddress = repNode.getSocketAddress();
        this.socketTimeout = socketTimeout;
    }

    /* Get the state of the specified node. */
    public NodeState getNodeState()
        throws IOException, ServiceConnectFailedException {

        BinaryNodeStateProtocol protocol =
            new BinaryNodeStateProtocol(NameIdPair.NOCHECK, null);
        SocketChannel channel = null;

        try {
            /* Build the connection. */
            channel = RepUtils.openBlockingChannel(socketAddress, 
                                                   true, 
                                                   socketTimeout);
            ServiceDispatcher.doServiceHandshake
                (channel, BinaryNodeStateService.SERVICE_NAME);

            /* Send a NodeState request to the node. */
            protocol.write
                (protocol.new BinaryNodeStateRequest(nodeName, groupName), 
                 channel);

            /* Get the response and return the NodeState. */
            BinaryNodeStateResponse response = 
                protocol.read(channel, BinaryNodeStateResponse.class);

            return response.convertToNodeState();
        } finally {
            if (channel != null) {
                channel.close();
            }
        }
    }
}
