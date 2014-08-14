package com.devsmart.moodb.query;

public class MergeCursor implements Cursor {

    private Cursor[] mCursor;
    private int i = 0;

    public MergeCursor(Cursor[] cursors) {
        mCursor = cursors;
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
        return false;
    }

    @Override
    public byte[] getData() {
        return mCursor[i].getData();
    }
}
