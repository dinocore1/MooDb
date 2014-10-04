/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.impl;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.impl.BinaryNodeStateProtocol.BinaryNodeStateRequest;
import com.sleepycat.je.rep.impl.BinaryNodeStateProtocol.BinaryNodeStateResponse;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.BinaryProtocol.ProtocolException;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ExecutingService;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * The service registered by a RepNode to answer the state request. 
 *
 * To support the new BinaryStateProtocol, we introduce this new 
 * BinaryNodeStateService, it's used by "Ping" command.
 *
 * Note: we can merge the two NodeState services together once we support
 * acitve version updates.
 */
public class BinaryNodeStateService extends ExecutingService {

    private final RepNode repNode;
    private final ServiceDispatcher dispatcher;
    private final Logger logger;

    /* Identifies the Node State querying Service. */
    public static final String SERVICE_NAME = "BinaryNodeState";

    public BinaryNodeStateService(ServiceDispatcher dispatcher, 
                                  RepNode repNode) {
        super(SERVICE_NAME, dispatcher);
        this.repNode = repNode;
        this.dispatcher = dispatcher;
        this.logger = LoggerUtils.getLogger(getClass());

        dispatcher.register(this);
    }

    public void shutdown() {
        dispatcher.cancel(SERVICE_NAME);
    }

    @Override
    public Runnable getRunnable(SocketChannel socketChannel) {
        return new NodeStateServiceRunnable(socketChannel);
    }

    class NodeStateServiceRunnable implements Runnable {
        private SocketChannel channel;

        NodeStateServiceRunnable(SocketChannel channel) {
            this.channel = channel;
        }

        /* Create the NodeState for the request. */
        private BinaryNodeStateResponse createResponse
            (BinaryNodeStateProtocol protocol) {

            long joinTime = repNode.getMonitorEventManager().getJoinTime();
            long txnEndVLSN = (repNode.getCurrentTxnEndVLSN() == null ?
                    0L : repNode.getCurrentTxnEndVLSN().getSequence());
            long masterTxnEndVLSN = repNode.replica().getMasterTxnEndVLSN();
            int activeFeeders = repNode.feederManager().activeReplicaCount();

            return protocol.new BinaryNodeStateResponse
                (repNode.getNodeName(), repNode.getGroup().getName(),
                 repNode.getMasterName(), JEVersion.CURRENT_VERSION, joinTime,
                 repNode.getRepImpl().getState(), txnEndVLSN, masterTxnEndVLSN,
                 activeFeeders, LogEntryType.LOG_VERSION, 
                 repNode.getAppState(), JVMSystemUtils.getSystemLoad());
        }

        public void run() {
            BinaryNodeStateProtocol protocol = null;

            try {
                protocol = new BinaryNodeStateProtocol(NameIdPair.NOCHECK, 
                                                       repNode.getRepImpl());
                try {
                    channel.configureBlocking(true);

                    BinaryNodeStateRequest msg = 
                        protocol.read(channel, BinaryNodeStateRequest.class);

                    /* 
                     * Response a protocol error if the group name doesn't 
                     * match. 
                     */
                    final String groupName = msg.getGroupName();
                    if (!repNode.getGroup().getName().equals(groupName) ||
                        !repNode.getNodeName().equals(msg.getNodeName())) {
                        throw new ProtocolException("Sending the request to" + 
                                " a wrong group or a wrong node.");
                    }

                    /* Write the response the requested node. */
                    BinaryNodeStateResponse response = 
                        createResponse(protocol);
                    protocol.write(response, channel);
                    LoggerUtils.finest(logger, repNode.getRepImpl(), 
                            "Deal with a node state request successfully.");
                } catch (ProtocolException e) {
                    LoggerUtils.info(logger, repNode.getRepImpl(),
                            "Get a ProtocolException with message: " + 
                            e.getMessage() + 
                            " while dealing with a node state request.");
                    protocol.write
                        (protocol.new ProtocolError(e.getMessage()), channel);
                } catch (Exception e) {
                    LoggerUtils.info(logger, repNode.getRepImpl(),
                            "Unexpected exception: " + e.getMessage());
                    protocol.write
                        (protocol.new ProtocolError(e.getMessage()), channel);
                } finally {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                }
            } catch (IOException e) {

                /*
                 * Channel has already been closed, or the close itself 
                 * failed.
                 */
            } 
        }
    }
}
