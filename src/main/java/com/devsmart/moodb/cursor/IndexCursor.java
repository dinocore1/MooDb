package com.devsmart.moodb.cursor;

import com.devsmart.moodb.MooDBCursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.RangeCursor;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.tree.CountEstimator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexCursor implements CountEstimateCursor {


    public static enum Direction {
        ASC,
        DESC
    }

    Logger logger = LoggerFactory.getLogger(IndexCursor.class);

    private SecondaryCursor mStartCursor;
    private SecondaryCursor mIndexCursor;
    private byte[] mKey;
    private final Direction mDirection;
    private long mLocation = BEFORE_FIRST;
    private DatabaseEntry data = new DatabaseEntry();


    public IndexCursor(SecondaryCursor cursor, byte[] key, Direction direction) {
        mIndexCursor = cursor;
        mDirection = direction;
        mKey = key;

        mIndexCursor.getSearchKeyRange(new DatabaseEntry(mKey), data, LockMode.DEFAULT);
        mLocation = 0;
        mStartCursor = mIndexCursor.dup(true);
    }

    @Override
    public long getCountEstimate() {
        long value = RangeCursor.countEstimate(mIndexCursor, mDirection);
        return value;
    }

    @Override
    public void reset() {
        mIndexCursor.close();
        mIndexCursor = mStartCursor;
        mStartCursor = mIndexCursor.dup(true);
        mLocation = 0;
    }

    @Override
    public boolean moveToNext() {
        if (mLocation == AFTER_LAST) {
            return false;
        }
        DatabaseEntry key = new DatabaseEntry();
        boolean success;
        if (mDirection == Direction.DESC){
            success = mIndexCursor.getPrevDup(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
        } else {
            success = mIndexCursor.getNextDup(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
        }
        if(success){
            mLocation++;
        }
        return success;
    }

    @Override
    public boolean moveToPrevious() {
        if(mLocation == BEFORE_FIRST){
            return false;
        }
        DatabaseEntry key = new DatabaseEntry();
        boolean success;
        if(mDirection == Direction.DESC) {
            success = mIndexCursor.getNextDup(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
        } else {
            success = mIndexCursor.getPrevDup(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
        }
        if(success){
            mLocation--;
        }
        return success;
    }

    @Override
    public String objectId() {
        return null;
    }

    @Override
    public byte[] getData() {
        return data.getData();
    }

    @Override
    public void close() {
        mIndexCursor.close();
        mStartCursor.close();
    }
}
