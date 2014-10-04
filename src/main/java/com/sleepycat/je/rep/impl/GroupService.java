/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.impl;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.impl.RepGroupProtocol.EnsureNode;
import com.sleepycat.je.rep.impl.RepGroupProtocol.FailReason;
import com.sleepycat.je.rep.impl.RepGroupProtocol.GroupRequest;
import com.sleepycat.je.rep.impl.RepGroupProtocol.RemoveMember;
import com.sleepycat.je.rep.impl.RepGroupProtocol.TransferMaster;
import com.sleepycat.je.rep.impl.RepGroupProtocol.UpdateAddress;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ExecutingService;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ExecutingRunnable;
import com.sleepycat.je.utilint.LoggerUtils;

public class GroupService extends ExecutingService {

    /* The replication node */
    final RepNode repNode;
    final RepGroupProtocol protocol;

    /**
     * List of sockets for in-flight requests.
     * The socket is in this collection while the request is being processed,
     * and must be removed before sending any response.
     *
     * @see #cancel
     * @see #unregisterSocket
     */
    private final Collection<SocketChannel> activeChannels =
        new ArrayList<SocketChannel>();

    private final Logger logger;

    /* Identifies the Group Service. */
    public static final String SERVICE_NAME = "Group";

    public GroupService(ServiceDispatcher dispatcher, RepNode repNode) {
        super(SERVICE_NAME, dispatcher);
        this.repNode = repNode;

        protocol = new RepGroupProtocol(repNode.getGroup().getName(),
                                        repNode.getNameIdPair(),
                                        repNode.getRepImpl());
        logger = LoggerUtils.getLogger(getClass());
    }
    
    @Override
    protected void cancel() {
        Collection<SocketChannel> channels;
        synchronized (this) {
            channels = new ArrayList<SocketChannel>(activeChannels);
            activeChannels.clear();
        }
        if (!channels.isEmpty()) {
            LoggerUtils.warning
                (logger, repNode.getRepImpl(),
                 "In-flight GroupService request(s) canceled: node shutdown");
        }
        for (SocketChannel channel : channels) {
            try {
                PrintWriter out =
                    new PrintWriter(channel.socket().getOutputStream(), true);
                ResponseMessage rm =
                    protocol.new Fail(FailReason.DEFAULT, "shutting down");
                out.println(rm.wireFormat());
            } catch (IOException e) {
                LoggerUtils.warning
                    (logger, repNode.getRepImpl(),
                     "IO error on socket: " + e.getMessage());
            } finally {
                if (channel.isOpen()) {
                    try {
                        channel.close();
                    }
                    catch (IOException e) {
                        LoggerUtils.warning
                            (logger, repNode.getRepImpl(),
                             "IO error on socket close: " + e.getMessage());
                    }
                }
            }
        }
    }

    /* Dynamically invoked process methods */

    /**
     * Wraps the replication group as currently cached on this node in
     * a Response message and returns it.
     */
    @SuppressWarnings("unused")
    public ResponseMessage process(GroupRequest groupRequest) {
        return protocol.new GroupResponse(repNode.getGroup());
    }

    /**
     * Ensures that the Monitor node, as described in the request, is a member
     * of the group.
     *
     * @param ensureNode the request message describing the monitor node
     *
     * @return EnsureOK message if the monitor node is already part of the rep
     * group, or was just made a part of the replication group. It returns a
     * Fail message if it could not be made part of the group. The message
     * associated with the response provides further details.
     */
    public ResponseMessage process(EnsureNode ensureNode) {
        RepNodeImpl node = ensureNode.getNode();
        try {
            repNode.getRepGroupDB().ensureMember(node);
            RepNodeImpl enode =
                repNode.getGroup().getMember(node.getName());
            return protocol.new EnsureOK(enode.getNameIdPair());
        } catch (DatabaseException e) {
            return protocol.new Fail(FailReason.DEFAULT, e.getMessage());
        }
    }

    /**
     * Removes a current member from the group.
     *
     * @param removeMember the request identifying the member to be removed.
     *
     * @return OK message if the member was removed from the group.
     */
    public ResponseMessage process(RemoveMember removeMember) {
        final String nodeName = removeMember.getNodeName();
        try {
            repNode.removeMember(nodeName);
            return protocol.new OK();
        } catch (MemberNotFoundException e) {
            return protocol.new Fail(FailReason.MEMBER_NOT_FOUND,
                                     e.getMessage());
        } catch (MasterStateException e) {
            return protocol.new Fail(FailReason.IS_MASTER, e.getMessage());
        }  catch (DatabaseException e) {
            return protocol.new Fail(FailReason.DEFAULT, e.getMessage());
        }
    }

    /**
     * Update the network address for a dead replica.
     *
     * @param updateAddress the request identifying the new network address for
     * the node.
     *
     * @return OK message if the address is successfully updated.
     */
    public ResponseMessage process(UpdateAddress updateAddress) {
        try {
            repNode.updateAddress(updateAddress.getNodeName(),
                                  updateAddress.getNewHostName(),
                                  updateAddress.getNewPort());
            return protocol.new OK();
        } catch (MemberNotFoundException e) {
            return protocol.new Fail(FailReason.MEMBER_NOT_FOUND,
                                     e.getMessage());
        } catch (MasterStateException e) {
            return protocol.new Fail(FailReason.IS_MASTER, e.getMessage());
        } catch (ReplicaStateException e) {
            return protocol.new Fail(FailReason.IS_ALIVE, e.getMessage());
        } catch (DatabaseException e) {
            return protocol.new Fail(FailReason.DEFAULT, e.getMessage());
        }
    }

    /**
     * Transfer the master role from the current master to one of the specified
     * replicas.
     *
     * @param transferMaster the request identifying nodes to be considered for
     * the role of new master
     * @return null
     */
    public ResponseMessage process(TransferMaster transferMaster) {
        try {
            final String nodeList = transferMaster.getNodeNameList();
            final Set<String> replicas = parseNodeList(nodeList);
            final long timeout = transferMaster.getTimeout();
            final boolean force = transferMaster.getForceFlag();
            String winner = repNode.transferMaster(replicas, timeout, force);
            return protocol.new TransferOK(winner);
        } catch (MasterTransferFailureException e) {
            return protocol.new Fail(FailReason.TRANSFER_FAIL, e.getMessage());
        } catch (DatabaseException e) {
            return protocol.new Fail(FailReason.DEFAULT, e.getMessage());
        } catch (IllegalArgumentException e) {
            return protocol.new Fail(FailReason.DEFAULT, e.toString());
        } catch (IllegalStateException e) {
            return protocol.new Fail(FailReason.DEFAULT, e.toString());
        }
    }
    
    private Set<String> parseNodeList(String list) {
        Set<String> set = new HashSet<String>();
        StringTokenizer st = new StringTokenizer(list, ",");
        while (st.hasMoreTokens()) {
            set.add(st.nextToken());
        }
        return set;
    }

    synchronized private void registerSocket(SocketChannel sc) {
        activeChannels.add(sc);
    }

    /**
     * Removes the given {@code SocketChannel} from our list of active sockets.
     * <p>
     * Before sending any response on the socket, this method must be invoked
     * to claim ownership of it.
     * This avoids a potential race between the request processing thread in
     * the normal case, and a thread calling {@code cancel()} at env shutdown
     * time.
     * 
     * @return true, if the socket is still active (usual case); false
     * otherwise, presumably because the service was shut down.
     */
    synchronized private boolean unregisterSocket(SocketChannel sc) {
        return activeChannels.remove(sc);
    }

    @Override
    public Runnable getRunnable(SocketChannel socketChannel) {
        return new GroupServiceRunnable(socketChannel, protocol);
    }

    class GroupServiceRunnable extends ExecutingRunnable {
        GroupServiceRunnable(SocketChannel socketChannel, 
                             RepGroupProtocol protocol) {
            super(socketChannel, protocol, true);
            registerSocket(socketChannel);
        }

        @Override
        protected ResponseMessage getResponse(RequestMessage request)  
            throws IOException {

            ResponseMessage rm = protocol.process(GroupService.this, request);

            /*
             * If the socket has already been closed, before we got a chance to
             * produce the response, then just discard the tardy response and
             * return null.
             */
            return unregisterSocket(channel) ? rm : null;
        }

        @Override
        protected void logMessage(String message) {
            LoggerUtils.warning(logger, repNode.getRepImpl(), message);
        }
    }
}
