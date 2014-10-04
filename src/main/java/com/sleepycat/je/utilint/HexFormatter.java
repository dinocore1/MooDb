/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.utilint;

public class HexFormatter {
    static public String formatLong(long l) {
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toHexString(l));
        sb.insert(0, "0000000000000000".substring(0, 16 - sb.length()));
        sb.insert(0, "0x");
        return sb.toString();
    }
}
