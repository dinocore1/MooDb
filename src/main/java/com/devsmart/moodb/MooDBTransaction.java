package com.devsmart.moodb;


import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MooDBTransaction {

    private Logger logger = LoggerFactory.getLogger(MooDBTransaction.class);
    private static final ThreadLocal<MooDBTransaction> currentTransaction = new ThreadLocal<MooDBTransaction>();

    public final Transaction mTr;
    private boolean mSuccess = false;
    private boolean mEnded = false;

    MooDBTransaction(Transaction tr) {
        mTr = tr;
    }

    static synchronized MooDBTransaction beginTransaction(Environment env) {
        MooDBTransaction tx = currentTransaction.get();
        if(tx == null) {
            tx = new MooDBTransaction(env.beginTransaction(null, null));
            currentTransaction.set(tx);
        }
        return tx;
    }

    public static synchronized Transaction getCurrentTransaction() {
        MooDBTransaction tx = currentTransaction.get();
        if(tx != null) {
            return tx.mTr;
        } else {
            return null;
        }
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
        synchronized(MooDBTransaction.class) {
            currentTransaction.set(null);
        }
    }

}
