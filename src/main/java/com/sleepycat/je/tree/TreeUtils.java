/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree;

/**
 * Miscellaneous Tree utilities.
 */
public class TreeUtils {

    static private final String SPACES =
        "                                " +
        "                                " +
        "                                " +
        "                                ";

    /**
     * For tree dumper.
     */
    public static String indent(int nSpaces) {
        return SPACES.substring(0, nSpaces);
    }
}
