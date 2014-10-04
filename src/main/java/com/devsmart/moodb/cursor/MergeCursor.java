package com.devsmart.moodb.cursor;

import com.devsmart.moodb.MooDBCursor;
import com.devsmart.moodb.objects.DBElement;

public class MergeCursor implements MooDBCursor {

    private MooDBCursor[] mCursor;
    private int i = 0;

    public MergeCursor(MooDBCursor[] cursors) {
        mCursor = cursors;
    }

    @Override
    public void reset() {
        for(MooDBCursor cursor : mCursor){
            cursor.reset();
        }
    }

    @Override
    public boolean moveToNext() {
        if(!mCursor[i].moveToNext() && i+1 < mCursor.length){
            i++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean moveToPrevious() {
        if(!mCursor[i].moveToPrevious() && i-1 >= 0){
            i--;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String objectId() {
        return null;
    }

    @Override
    public byte[] getData() {
        return mCursor[i].getData();
    }

    @Override
    public DBElement getDBElement() {
        return mCursor[i].getDBElement();
    }

    @Override
    public void close() {
        for(MooDBCursor cursor : mCursor) {
            cursor.close();
        }
    }
}
