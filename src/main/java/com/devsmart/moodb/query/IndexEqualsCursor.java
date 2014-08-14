package com.devsmart.moodb.query;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;

import java.util.Arrays;

public class IndexEqualsCursor implements Cursor {

    final Direction mDirection;
    private final SecondaryCursor mIndexCursor;
    private final byte[] mKey;
    private long mLocation = BEFORE_FIRST;
    private DatabaseEntry data = new DatabaseEntry();


    public IndexEqualsCursor(SecondaryCursor cursor, byte[] key, Direction direction) {
        mIndexCursor = cursor;
        mDirection = direction;
        mKey = key;
        if(mIndexCursor.getSearchKey(new DatabaseEntry(key), data, LockMode.DEFAULT) != OperationStatus.SUCCESS){
            mLocation = 0;
        }
    }

    @Override
    public boolean moveToNext() {
        if(mLocation == AFTER_LAST){
            return false;
        }
        DatabaseEntry key = new DatabaseEntry();
        if(mDirection == Direction.ASC){
            mIndexCursor.getNextDup(key, data, LockMode.DEFAULT);
        } else {
            mIndexCursor.getPrevDup(key, data, LockMode.DEFAULT);
        }
        boolean success = Arrays.equals(mKey, key.getData());
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
        if(mDirection == Direction.ASC){
            mIndexCursor.getPrevDup(key, data, LockMode.DEFAULT);
        } else {
            mIndexCursor.getNextDup(key, data, LockMode.DEFAULT);
        }
        boolean success = Arrays.equals(mKey, key.getData());
        if(success){
            mLocation--;
        }
        return success;
    }

    @Override
    public byte[] getData() {
        return data.getData();
    }
}
