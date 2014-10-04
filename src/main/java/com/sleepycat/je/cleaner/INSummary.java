/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.cleaner;

/**
 * Used to trace the relative numbers of full INs and BINDeltas that are
 * obsolete vs active.  May be used in the future for adjusting utilization.
 */
public class INSummary {
    public int totalINCount;
    public int totalINSize;
    public int totalBINDeltaCount;
    public int totalBINDeltaSize;
    public int obsoleteINCount;
    public int obsoleteINSize;
    public int obsoleteBINDeltaCount;
    public int obsoleteBINDeltaSize;

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("<INSummary totalINCount=\"");
        buf.append(totalINCount);
        buf.append("\" totalINSize=\"");
        buf.append(totalINSize);
        buf.append("\" totalBINDeltaCount=\"");
        buf.append(totalBINDeltaCount);
        buf.append("\" totalBINDeltaSize=\"");
        buf.append(totalBINDeltaSize);
        buf.append("\" obsoleteINCount=\"");
        buf.append(obsoleteINCount);
        buf.append("\" obsoleteINSize=\"");
        buf.append(obsoleteINSize);
        buf.append("\" obsoleteBINDeltaCount=\"");
        buf.append(obsoleteBINDeltaCount);
        buf.append("\" obsoleteBINDeltaSize=\"");
        buf.append(obsoleteBINDeltaSize);
        buf.append("\"/>");

        return buf.toString();
    }
}
