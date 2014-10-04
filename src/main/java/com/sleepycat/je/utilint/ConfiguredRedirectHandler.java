/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002,2012 Oracle.  All rights reserved.
 *
 */

package com.sleepycat.je.utilint;

import java.util.logging.Handler;
import java.util.logging.LogRecord;

import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Redirects logging messages to the owning environment's application
 * configured handler, if one was specified through
 * EnvironmentConfig.setLoggingHandler(). Handlers for JE logging can be
 * configured through EnvironmentConfig, to support handlers which:
 * - require a constructor with arguments
 * - is specific to this environment, and multiple environments exist in the
 *   same process.
 */
public class ConfiguredRedirectHandler extends Handler {

    public ConfiguredRedirectHandler() {
        /* No need to call super, this handler is not truly publishing. */
    }

    @Override
    public void publish(LogRecord record) {
        Handler h = getEnvSpecificConfiguredHandler();
        if ((h != null) && (h.isLoggable(record))) {
            h.publish(record);
        }
    }

    private Handler getEnvSpecificConfiguredHandler() {
        EnvironmentImpl envImpl =
            LoggerUtils.envMap.get(Thread.currentThread());

        /*
         * Prefer to lose logging output, rather than risk a
         * NullPointerException if the caller forgets to set and release the
         * environmentImpl.
         */
        if (envImpl == null) {
            return null;
        }

        return envImpl.getConfiguredHandler();
    }

    @Override
    public void close()
        throws SecurityException {
        Handler h = getEnvSpecificConfiguredHandler();
        if (h != null) {
            h.close();
        }
    }

    @Override
    public void flush() {
        Handler h = getEnvSpecificConfiguredHandler();
        if (h != null) {
            h.flush();
        }
    }
}
