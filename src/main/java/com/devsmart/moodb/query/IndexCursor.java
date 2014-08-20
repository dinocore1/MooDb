package com.devsmart.moodb.query;

import com.devsmart.moodb.MooDBCursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexCursor implements MooDBCursor {

    Logger logger = LoggerFactory.getLogger(IndexCursor.class);

    private SecondaryCursor mStartCursor;
    private SecondaryCursor mIndexCursor;
    private byte[] mKey;
    private long mLocation = BEFORE_FIRST;
    private DatabaseEntry data = new DatabaseEntry();

    public IndexCursor(SecondaryCursor cursor) {
        mIndexCursor = cursor;
        mStartCursor = cursor.dup(true);
    }

    public IndexCursor(SecondaryCursor cursor, byte[] key, Direction direction) {
        mIndexCursor = cursor;

        OperationStatus status = mIndexCursor.getSearchKey(new DatabaseEntry(mKey), data, LockMode.DEFAULT);
        if(status != OperationStatus.SUCCESS){
            String errorStr = String.format("error searching for key: %s", status);
            logger.error(errorStr);
            throw new RuntimeException(errorStr);
        }

        mStartCursor = mIndexCursor.dup(true);
        mKey = key;
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
        if(mLocation == AFTER_LAST){
            return false;
        }
        DatabaseEntry key = new DatabaseEntry();
        boolean success;
        success = mIndexCursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
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
        success = mIndexCursor.getPrev(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
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
