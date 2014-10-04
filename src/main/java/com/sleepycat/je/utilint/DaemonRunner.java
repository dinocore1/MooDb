/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.utilint;

import com.sleepycat.je.ExceptionListener;

/**
 * An object capable of running (run/pause/shutdown/etc) a daemon thread.
 * See DaemonThread for details.
 */
public interface DaemonRunner {
    void setExceptionListener(ExceptionListener exceptionListener);
    void runOrPause(boolean run);
    void requestShutdown();
    void shutdown();
    int getNWakeupRequests();
}
