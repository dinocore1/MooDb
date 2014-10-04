package com.devsmart.moodb.cursor;


import com.devsmart.moodb.Utils;
import com.devsmart.moodb.objects.DBElement;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class AllObjectsCursor implements CountEstimateCursor {

    private Cursor mCursor;
    private Cursor mDup;
    private DatabaseEntry key = new DatabaseEntry();
    private DatabaseEntry data = new DatabaseEntry();
    private long mLocation = BEFORE_FIRST;

    public AllObjectsCursor(Cursor cursor) {
        mCursor = cursor;
        mDup = cursor.dup(true);
    }

    @Override
    public void reset() {
        mCursor = mDup.dup(true);
        mDup = mCursor.dup(true);
    }

    @Override
    public boolean moveToNext() {
        boolean retval = mCursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
        return retval;
    }

    @Override
    public boolean moveToPrevious() {
        boolean retval = mCursor.getPrev(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
        return retval;
    }

    @Override
    public String objectId() {
        return Utils.toString(key.getData());
    }

    @Override
    public byte[] getData() {
        return data.getData();
    }

    @Override
    public DBElement getDBElement() {
        return Utils.toDBElement(data);
    }

    @Override
    public void close() {
        if(mDup != null){
            mDup.close();
        }
        if(mCursor != null) {
            mCursor.close();
        }
    }

    @Override
    public long getCountEstimate() {
        return Long.MAX_VALUE;
    }
}
