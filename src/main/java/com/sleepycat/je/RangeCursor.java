package com.sleepycat.je;


import com.devsmart.moodb.cursor.IndexCursor;
import com.sleepycat.je.tree.CountEstimator;

public class RangeCursor {

    public static long countEstimate(SecondaryCursor begining, IndexCursor.Direction direction) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        begining = begining.dup(true);
        SecondaryCursor endengin = begining.dup(false);
        try {
            if (direction == IndexCursor.Direction.ASC) {
                endengin.getLast(key, value, LockMode.READ_UNCOMMITTED);
            } else {
                endengin.getFirst(key, value, LockMode.READ_UNCOMMITTED);
                SecondaryCursor tmp = endengin;
                endengin = begining;
                begining = tmp;
            }
            return CountEstimator.count(begining.getDatabaseImpl(), begining.cursorImpl, true, endengin.cursorImpl, true);
        } finally {
            begining.close();
            endengin.close();
        }
    }

}
