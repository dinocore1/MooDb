/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2010, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.utilint;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * An iterator to iterate over the free ports on an interface.
 */
public class FreePortLocator {

    private final String hostname;
    private final int portStart;
    private final int portEnd;

    private int currPort;

    /**
     * Constructor identifying the interface and the port range within which
     * to look for free ports.
     */
    public FreePortLocator(String hostname, int portStart, int portEnd) {
        super();
        assert portStart < portEnd;
        this.hostname = hostname;
        this.portStart = portStart;
        this.portEnd = portEnd;
        currPort = portStart;
    }

    /**
     * Returns the next free port. Note that it's possible that on a busy
     * machine another process may grab the "free" port before it's actually
     * used.
     */
    public int next() {
        ServerSocket serverSocket = null;
        while (++currPort < portEnd) {

            try {

                /**
                 * Try a couple different methods to be sure that the port is
                 * truly available.
                 */
                serverSocket = new ServerSocket(currPort);
                serverSocket.close();

                /**
                 * Now using the hostname.
                 */
                serverSocket = new ServerSocket();
                InetSocketAddress sa =
                    new InetSocketAddress(hostname, currPort);
                serverSocket.bind(sa);
                serverSocket.close();
                return currPort;
            } catch (IOException e) {
                /* Try the next port number. */
                continue;
            }
        }

        throw new IllegalStateException
            ("No more ports avaliable in the range: " +
             portStart + " - " + portEnd);
    }

    /**
     * Skip a number of ports.
     */
    public void skip(int num) {
        currPort += num;
    }
}
