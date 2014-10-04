/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.dbi;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.tree.FileSummaryLN;
import com.sleepycat.je.tree.LN;

/**
 * Classifies all databases as specific internal databases or user databases.
 * This can be thought of as a substitute for having DatabaseImpl subclasses
 * for different types of databases.  It also identifies each internal database
 * by name.
 */
public enum DbType {

    ID("_jeIdMap") {
        @Override
        public boolean mayCreateDeletedLN() {
            return false;
        }
        @Override
        public LN createDeletedLN(EnvironmentImpl envImpl) {
            throw EnvironmentFailureException.unexpectedState();
        }
        @Override
        public boolean mayCreateUpdatedLN() {
            return false;
        }
        @Override
        public LN createUpdatedLN(EnvironmentImpl envImpl, byte[] newData) {
            throw EnvironmentFailureException.unexpectedState();
        }
    },

    NAME("_jeNameMap") {
        @Override
        public boolean mayCreateDeletedLN() {
            return false;
        }
        @Override
        public LN createDeletedLN(EnvironmentImpl envImpl) {
            throw EnvironmentFailureException.unexpectedState();
        }
        @Override
        public boolean mayCreateUpdatedLN() {
            return false;
        }
        @Override
        public LN createUpdatedLN(EnvironmentImpl envImpl, byte[] newData) {
            throw EnvironmentFailureException.unexpectedState();
        }
    },

    UTILIZATION("_jeUtilization") {
        @Override
        public LN createDeletedLN(EnvironmentImpl envImpl) {
            return FileSummaryLN.makeDeletedLN();
        }
        @Override
        public boolean mayCreateUpdatedLN() {
            return false;
        }
        @Override
        public LN createUpdatedLN(EnvironmentImpl envImpl, byte[] newData) {
            throw EnvironmentFailureException.unexpectedState();
        }
    },

    REP_GROUP("_jeRepGroupDB"),

    VLSN_MAP("_jeVlsnMapDb"),

    SYNC("_jeSyncDb"),

    USER(null);

    private final String internalName;

    private DbType(String internalName) {
        this.internalName = internalName;
    }

    /**
     * Returns true if this is an internal DB, or false if it is a user DB.
     */
    public boolean isInternal() {
        return internalName != null;
    }

    /**
     * Returns the DB name for an internal DB type.
     *
     * @throws EnvironmentFailureException if this is not an internal DB type.
     */
    public String getInternalName() {
        if (internalName == null) {
            throw EnvironmentFailureException.unexpectedState();
        }
        return internalName;
    }

    /**
     * Returns true if createUpdatedLN may be called.
     */
    public boolean mayCreateUpdatedLN() {
        return true;
    }

    /**
     * Creates an updated LN for use in an optimization in
     * CursorImpl.putCurrentAlreadyLatchedAndLocked.  Without this method it
     * would be necessary to fetch the existing LN and call LN.modify.
     *
     * @throws EnvironmentFailureException if this is not allowed.
     */
    public LN createUpdatedLN(EnvironmentImpl envImpl, byte[] newData) {
        return LN.makeLN(envImpl, newData);
    }

    /**
     * Returns true if createDeletedLN may be called.
     */
    public boolean mayCreateDeletedLN() {
        return true;
    }

    /**
     * Creates a deleted LN for use in an optimization in CursorImpl.delete.
     * Without this method it would be necessary to fetch the existing LN and
     * call LN.delete.
     *
     * @throws EnvironmentFailureException if this is not allowed.
     */
    public LN createDeletedLN(EnvironmentImpl envImpl) {
        return LN.makeLN(envImpl, (byte[]) null);
    }
}
