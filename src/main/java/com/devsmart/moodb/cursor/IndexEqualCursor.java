package com.devsmart.moodb.cursor;

import com.devsmart.moodb.MooDBCursor;
import com.devsmart.moodb.Utils;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


public class IndexEqualCursor implements MooDBCursor {

    private final OperationStatus mStatus;
    Logger logger = LoggerFactory.getLogger(IndexEqualCursor.class);

    private SecondaryCursor mStartCursor;
    private SecondaryCursor mIndexCursor;
    private long mLocation = BEFORE_FIRST;
    private final byte[] mKeyData;
    private DatabaseEntry data = new DatabaseEntry();
    private DatabaseEntry key = new DatabaseEntry();
    private DatabaseEntry pkey = new DatabaseEntry();

    public IndexEqualCursor(SecondaryCursor cursor, byte[] keydata) {
        mKeyData = keydata;
        mIndexCursor = cursor;

        key.setData(mKeyData);
        mStatus = mIndexCursor.getSearchKey(key, pkey, data, LockMode.DEFAULT);
        if(mStatus == OperationStatus.SUCCESS){
            mStartCursor = mIndexCursor.dup(true);
        }

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
        if(mStatus == OperationStatus.NOTFOUND) {
            return false;
        }
        if(mLocation == BEFORE_FIRST){
            mLocation = 0;
            return true;
        }
        if(mLocation == AFTER_LAST){
            return false;
        }
        boolean success = mIndexCursor.getNextDup(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
        if(!Arrays.equals(key.getData(), mKeyData)){
            success = false;
            mLocation = AFTER_LAST;
        }
        if(success){
            mLocation++;
        }
        return success;
    }

    @Override
    public boolean moveToPrevious() {
        if(mStatus == OperationStatus.NOTFOUND) {
            return false;
        }
        if(mLocation == BEFORE_FIRST){
            return false;
        }
        boolean success = mIndexCursor.getPrevDup(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
        if(!Arrays.equals(key.getData(), mKeyData)){
            success = false;
            mLocation = BEFORE_FIRST;
        }

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
        if(mStartCursor != null) {
            mStartCursor.close();
        }
        mIndexCursor.close();
    }
}
