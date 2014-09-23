package com.devsmart.moodb.cursor;


import com.devsmart.moodb.Utils;
import com.devsmart.moodb.objectquery.ObjectOperation;
import com.devsmart.moodb.objects.DBElement;

public class ObjectOperationCursor implements CountEstimateCursor {

    private final CountEstimateCursor mCursor;
    private final ObjectOperation mOperation;
    private DBElement mObj;
    private byte[] mData;

    public ObjectOperationCursor(CountEstimateCursor cursor, ObjectOperation operation) {
        mCursor = cursor;
        mOperation = operation;
    }


    @Override
    public void reset() {
        mCursor.reset();
    }

    @Override
    public boolean moveToNext() {
        if(mCursor.moveToNext()){
            mData = mCursor.getData();
            mObj = mOperation.eval(Utils.toDBElement(mData));
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean moveToPrevious() {
        if(mCursor.moveToPrevious()){
            mData = mCursor.getData();
            mObj = mOperation.eval(Utils.toDBElement(mData));
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String objectId() {
        return mCursor.objectId();
    }

    @Override
    public byte[] getData() {
        return mData;
    }

    @Override
    public void close() {
        if(mCursor != null) {
            mCursor.close();
        }
    }

    @Override
    public long getCountEstimate() {
        return mCursor.getCountEstimate();
    }
}
