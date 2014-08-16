package com.devsmart.moodb.query;

import com.devsmart.moodb.MooDB;
import com.devsmart.moodb.MooDBCursor;

public class JoinCursor implements MooDBCursor {

    private final MooDBCursor[] mCurors;
    private final MooDB mMooDB;
    private String mObjectId;

    public JoinCursor(MooDB db, MooDBCursor[] cursors) {
        mMooDB = db;
        mCurors = cursors;
    }

    @Override
    public void reset() {
        mCurors[0].reset();
    }

    @Override
    public boolean moveToNext() {
        boolean found = false;
        while(mCurors[0].moveToNext()) {
            String objectId = mCurors[0].objectId();

            for (int i = 1; i < mCurors.length; i++) {
                mCurors[i].reset();
                found = false;
                while (mCurors[i].moveToNext()) {
                    if (mCurors[i].objectId().equals(objectId)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    break;
                }
            }

            if(found){
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean moveToPrevious() {
        boolean found = false;
        while(mCurors[0].moveToPrevious()) {
            mObjectId = mCurors[0].objectId();

            for (int i = 1; i < mCurors.length; i++) {
                mCurors[i].reset();
                found = false;
                while (mCurors[i].moveToNext()) {
                    if (mCurors[i].objectId().equals(mObjectId)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    break;
                }
            }

            if(found){
                return true;
            }
        }

        return false;
    }

    @Override
    public String objectId() {
        return mObjectId;
    }

    @Override
    public byte[] getData() {
        return mMooDB.get(mObjectId);
    }
}
