package com.devsmart.moodb.cursor;

import com.devsmart.moodb.Utils;
import com.devsmart.moodb.objectquery.Predicate;
import com.devsmart.moodb.objects.DBElement;

public class PredicateAndCursor implements CountEstimateCursor {

    private final CountEstimateCursor mCursor;
    private final Predicate mPredicate;
    private byte[] mData;

    public PredicateAndCursor(CountEstimateCursor cursor, Predicate predicate) {
        mCursor = cursor;
        mPredicate = predicate;
    }

    @Override
    public void reset() {
        mCursor.reset();
    }

    @Override
    public boolean moveToNext() {
        boolean retval = false;
        while(mCursor.moveToNext()){
            mData = mCursor.getData();
            DBElement element = Utils.toDBElement(mData);
            if(mPredicate.matches(element)){
                retval = true;
                break;
            }
        }
        return retval;
    }

    @Override
    public boolean moveToPrevious() {
        boolean retval = false;
        while(mCursor.moveToPrevious()){
            mData = mCursor.getData();
            DBElement element = Utils.toDBElement(mData);
            if(mPredicate.matches(element)){
                retval = true;
                break;
            }
        }
        return retval;
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
    public DBElement getDBElement() {
        return Utils.toDBElement(mData);
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
