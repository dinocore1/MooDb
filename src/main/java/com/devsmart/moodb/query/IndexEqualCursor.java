package com.devsmart.moodb.query;

import com.devsmart.moodb.MooDBCursor;
import com.devsmart.moodb.Utils;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IndexEqualCursor implements MooDBCursor {

    Logger logger = LoggerFactory.getLogger(IndexEqualCursor.class);

    private SecondaryCursor mStartCursor;
    private SecondaryCursor mIndexCursor;
    private long mLocation = BEFORE_FIRST;
    private final DatabaseEntry key;
    private DatabaseEntry data = new DatabaseEntry();
    private DatabaseEntry pkey = new DatabaseEntry();

    public IndexEqualCursor(SecondaryCursor cursor, DatabaseEntry key) {
        this.key = key;
        mIndexCursor = cursor;

        OperationStatus status = mIndexCursor.getSearchKey(key, pkey, data, LockMode.DEFAULT);
        if(status != OperationStatus.SUCCESS){
            String errorStr = String.format("error searching for key: %s", status);
            logger.error(errorStr);
            throw new RuntimeException(errorStr);
        }
        mStartCursor = mIndexCursor.dup(true);
    }

    @Override
    public void reset() {
        mIndexCursor.close();
        mIndexCursor = mStartCursor;
        mStartCursor = mIndexCursor.dup(true);
        mLocation = BEFORE_FIRST;
    }

    @Override
    public boolean moveToNext() {
        if(mLocation == BEFORE_FIRST){
            mLocation = 0;
            return true;
        }
        if(mLocation == AFTER_LAST){
            return false;
        }
        boolean success = mIndexCursor.getNextDup(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
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
        boolean success = mIndexCursor.getPrevDup(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;

        if(success){
            mLocation--;
        }
        return success;
    }

    @Override
    public String objectId() {
        return Utils.toString(pkey);
    }

    @Override
    public byte[] getData() {
        return data.getData();
    }

    @Override
    public void close() {
        mStartCursor.close();
        mIndexCursor.close();
    }
}
