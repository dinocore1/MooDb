/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.util;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.PropUtil;

/**
 * DbGroupAdmin supplies the functionality of the administrative class {@link
 * ReplicationGroupAdmin} in a convenient command line utility. For example, it
 * can be used to display replication group information, or to remove a node
 * from the replication group.
 * <p>
 * Note: This utility does not handle security and authorization. It is left
 * to the user to ensure that the utility is invoked with proper authorization.
 * <p>
 * See {@link DbGroupAdmin#main} for a full description of the command line
 * arguments.
 */
public class DbGroupAdmin {

    enum Command { DUMP, REMOVE, TRANSFER_MASTER, UPDATE_ADDRESS };

    private String groupName;
    private Set<InetSocketAddress> helperSockets;
    private String nodeName;
    private String newHostName;
    private int newPort;
    private String timeout;
    private boolean forceFlag;
    private ReplicationGroupAdmin groupAdmin;
    private final ArrayList<Command> actions = new ArrayList<Command>();

    private static final String usageString =
        "Usage: " + CmdUtil.getJavaCommand(DbGroupAdmin.class) + "\n" +
        "  -groupName <group name>   # name of replication group\n" +
        "  -helperHosts <host:port>  # identifier for one or more members\n" +
        "                            # of the replication group which can\n"+
        "                            # be contacted for group information,\n"+
        "                            # in this format:\n" +
        "                            # hostname[:port][,hostname[:port]]\n" +
        "  -dumpGroup                # dump group information\n" +
        "  -removeMember <node name> # node to be removed\n" +
        "  -updateAddress <node name> <new host:port>\n" +
        "                            # update the network address for a\n " +
        "                            # specified node.  The node should not\n" +
        "                            # be alive when updating the address\n" +
        "  -transferMaster [-force] <node1,node2,...> <timeout>\n" +
        "                            # transfer master role to one of the\n" +
        "                            # specified nodes.";

    /**
     * Usage:
     * <pre>
     * java {com.sleepycat.je.rep.util.DbGroupAdmin |
     *       -jar je-&lt;version&gt;.jar DbGroupAdmin}
     *   -groupName &lt;group name&gt;  # name of replication group
     *   -helperHosts &lt;host:port&gt; # identifier for one or more members
     *                            # of the replication group which can be
     *                            # contacted for group information, in
     *                            # this format:
     *                            # hostname[:port][,hostname[:port]]*
     *   -dumpGroup               # dump group information
     *   -removeMember &lt;node name&gt;# node to be removed
     *   -updateAddress &lt;node name&gt; &lt;new host:port&gt;
     *                            # update the network address for a specified
     *                            # node. The node should not be alive when
     *                            # updating address
     *   -transferMaster [-force] &lt;node1,node2,...&gt; &lt;timeout&gt;                         
     * </pre>
     */
    public static void main(String... args)
        throws Exception {

        DbGroupAdmin admin = new DbGroupAdmin();
        admin.parseArgs(args);
        admin.run();
    }

    /**
     * Print usage information for this utility.
     *
     * @param message
     */
    private void printUsage(String msg) {
        if (msg != null) {
            System.out.println(msg);
        }

        System.out.println(usageString);
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
            System.exit(0);
        }

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-groupName")) {
                if (argc < nArgs) {
                    groupName = argv[argc++];
                } else {
                    printUsage("-groupName requires an argument");
                }
            } else if (thisArg.equals("-helperHosts")) {
                if (argc < nArgs) {
                    helperSockets = HostPortPair.getSockets(argv[argc++]);
                } else {
                    printUsage("-helperHosts requires an argument");
                }
            } else if (thisArg.equals("-dumpGroup")) {
                actions.add(Command.DUMP);
            } else if (thisArg.equals("-removeMember")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                    actions.add(Command.REMOVE);
                } else {
                    printUsage("-removeMember requires an argument");
                }
            } else if (thisArg.equals("-updateAddress")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];

                    if (argc < nArgs) {
                        String hostPort = argv[argc++];
                        int index = hostPort.indexOf(":");
                        if (index < 0) {
                            printUsage("Host port pair format must be " +
                                       "<host name>:<port number>");
                        }

                        newHostName = hostPort.substring(0, index);
                        newPort = Integer.parseInt
                            (hostPort.substring(index + 1, hostPort.length()));
                    } else {
                        printUsage("-updateAddress requires a " +
                                   "<host name>:<port number> argument");
                    }

                    actions.add(Command.UPDATE_ADDRESS);
                } else {
                    printUsage
                        ("-updateAddress requires the node name argument");
                }
            } else if (thisArg.equals("-transferMaster")) {

                // TODO: it wouldn't be too hard to allow "-force" as a
                // node name.
                // 
                if (argc < nArgs && "-force".equals(argv[argc])) {
                    forceFlag = true;
                    argc++;
                }
                if (argc + 1 < nArgs) {
                    nodeName = argv[argc++];

                    /*
                     * Allow either
                     *     -transferMaster mercury,venus 900 ms
                     * or
                     *     -transferMaster mercury,venus "900 ms"
                     */
                    if (argc + 1 < nArgs && argv[argc + 1].charAt(0) != '-') {
                        timeout = argv[argc] + " " + argv[argc + 1];
                        argc += 2;
                    } else {
                        timeout = argv[argc++];
                    }

                    actions.add(Command.TRANSFER_MASTER);
                } else {
                    printUsage
                        ("-transferMaster requires at least two arguments");
                }
            } else {
                printUsage(thisArg + " is not a valid argument");
            }
        }
    }

    /* Execute commands */
    private void run()
        throws Exception {

        createGroupAdmin();

        if (actions.size() == 0) {
            return;
        }

        for (Command action : actions) {
            /* Dump the group information. */
            if (action == Command.DUMP) {
                dumpGroup();
            }

            /* Remove a member. */
            if (action == Command.REMOVE) {
                removeMember(nodeName);
            }

            /* Transfer the current mastership to a specified node. */
            if (action == Command.TRANSFER_MASTER) {
                transferMaster(nodeName, timeout);
            }

            /* Update the network address of a specified node. */
            if (action == Command.UPDATE_ADDRESS) {
                updateAddress(nodeName, newHostName, newPort);
            }
        }
    }

    private DbGroupAdmin() {
    }

    /**
     * Create a DbGroupAdmin instance for programmatic use.
     *
     * @param groupName replication group name
     * @param helperSockets set of host and port pairs for group members which
     * can be queried to obtain group information.
     */
    public DbGroupAdmin(String groupName,
                        Set<InetSocketAddress> helperSockets) {
        this.groupName = groupName;
        this.helperSockets = helperSockets;
        createGroupAdmin();
    }

    /* Create the ReplicationGroupAdmin object. */
    private void createGroupAdmin() {
        if (groupName == null) {
            printUsage("Group name must be specified");
        }

        if ((helperSockets == null) || (helperSockets.size() == 0)) {
            printUsage("Host and ports of helper nodes must be specified");
        }

        groupAdmin = new ReplicationGroupAdmin(groupName, helperSockets);
    }

    /**
     * Display group information. Lists all members and the group master.  Can
     * be used when reviewing the <a
     * href="http://www.oracle.com/technetwork/database/berkeleydb/je-faq-096044.html#HAChecklist">group configuration. </a>
     */
    public void dumpGroup() {
        System.out.println(getFormattedOutput());
    }

    /**
     * Remove a node from the replication group. Once removed, a
     * node cannot be added again to the group under the same node name.
     *
     * @param name name of the node to be removed
     *
     * @see ReplicationGroupAdmin#removeMember
     */
    public void removeMember(String name) {
        if (name == null) {
            printUsage("Node name must be specified");
        }

        groupAdmin.removeMember(name);
    }

    /**
     * Update the network address for a specified node. When updating the
     * address of a node, the node cannot be alive. See {@link
     * ReplicationGroupAdmin#updateAddress} for more information.
     *
     * @param nodeName the name of the node whose address will be updated
     * @param newHostName the new host name of the node
     * @param newPort the new port number of the node
     */
    public void updateAddress(String nodeName,
                              String newHostName,
                              int newPort) {
        if (nodeName == null || newHostName == null) {
            printUsage("Node name and new host name must be specified");
        }

        if (newPort <= 0) {
            printUsage("Port of the new network address must be specified");
        }

        groupAdmin.updateAddress(nodeName, newHostName, newPort);
    }

    /**
     * Transfers the master role from the current master to one of the replicas
     * specified in the argument list.
     *
     * @param nodeList comma-separated list of nodes
     * @param timeout in <a href="../../EnvironmentConfig.html#timeDuration">
     *        same form</a> as accepted by duration config params
     *
     * @see ReplicatedEnvironment#transferMaster
     */
    public void transferMaster(String nodeList, String timeout) {
        String result =
            groupAdmin.transferMaster(parseNodes(nodeList),
                                      PropUtil.parseDuration(timeout),
                                      TimeUnit.MILLISECONDS,
                                      forceFlag);
        System.out.println("The new master is: " + result);
    }

    private Set<String> parseNodes(String nodes) {
        if (nodes == null) {
            throw new IllegalArgumentException("node list may not be null");
        }
        StringTokenizer st = new StringTokenizer(nodes, ",");
        Set<String> set = new HashSet<String>();
        while (st.hasMoreElements()) {
            set.add(st.nextToken());
        }
        return set;
    }

    /*
     * This method presents group information in a user friendly way. Internal
     * fields are hidden.
     */
    private String getFormattedOutput() {
        StringBuilder sb = new StringBuilder();
        RepGroupImpl repGroupImpl = groupAdmin.getGroup().getRepGroupImpl();

        /* Get the master node name. */
        String masterName = groupAdmin.getMasterNodeName();

        /* Get the electable nodes information. */
        sb.append("\nGroup: " + repGroupImpl.getName() + "\n");
        sb.append("Electable Members:\n");
        Set<RepNodeImpl> nodes = repGroupImpl.getAllElectableMembers();
        if (nodes.size() == 0) {
            sb.append("    No electable members\n");
        } else {
            for (RepNodeImpl node : nodes) {
                String type =
                    masterName.equals(node.getName()) ? "master, " : "";
                sb.append("    " + node.getName() + " (" + type +
                          node.getHostName() + ":" + node.getPort() + ", " +
                          node.getBarrierState() + ")\n");
            }
        }

        /* Get the monitors information. */
        sb.append("\nMonitor Members:\n");
        nodes = repGroupImpl.getMonitorNodes();
        if (nodes.size() == 0) {
            sb.append("    No monitors\n");
        } else {
            for (RepNodeImpl node : nodes) {
                sb.append("    " + node.getName() + " (" + node.getHostName() +
                          ":" + node.getPort() + ")\n");
            }
        }

        return sb.toString();
    }
}
