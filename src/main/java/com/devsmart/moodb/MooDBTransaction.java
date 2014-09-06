package com.devsmart.moodb;


import com.sleepycat.je.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MooDBTransaction {

    private Logger logger = LoggerFactory.getLogger(MooDBTransaction.class);

    private final Transaction mTr;
    private boolean mSuccess = false;
    private boolean mEnded = false;

    MooDBTransaction(Transaction tr) {
        mTr = tr;
    }

    @Override
    protected void finalize() throws Throwable {
        if(!mEnded) {
            logger.warn("endTransaction not called");
        }
        endTransaction();
        super.finalize();
    }

    public void setTransactionSuccessfull() {
        mSuccess = true;
    }

    public void endTransaction() {
        if(!mEnded) {
            if (mSuccess) {
                mTr.commit();
            } else {
                mTr.abort();
            }
            mEnded = true;
        }
    }

}
