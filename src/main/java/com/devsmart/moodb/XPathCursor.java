package com.devsmart.moodb;


import com.google.gson.JsonElement;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import org.apache.commons.jxpath.CompiledExpression;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.JXPathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

public class XPathCursor implements MooDBCursor, Closeable {

    private Logger logger = LoggerFactory.getLogger(XPathCursor.class);

    private final MooDBCursor mCursor;
    private final CompiledExpression mXPath;
    private final MooDB mMooDBContext;

    private Object mResultObj;
    private boolean mIsClosed = false;

    protected XPathCursor(MooDB db, MooDBCursor cursor, CompiledExpression xpath) {
        mMooDBContext = db;
        mCursor = cursor;
        mXPath = xpath;
    }

    @Override
    public void reset() {
        mCursor.reset();
    }

    @Override
    public boolean moveToNext() {
        while(mCursor.moveToNext()){
            if(queryObject() != null){
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean moveToPrevious() {
        while(mCursor.moveToPrevious()){
            if(queryObject() != null){
                return true;
            }
        }
        return false;
    }

    @Override
    public String objectId() {
        return null;
    }

    private Object queryObject() {
        Object retval = null;
        JsonElement jsonElement = mMooDBContext.gson.fromJson(Utils.toString(mCursor.getData()), JsonElement.class);
        Object xpathObj = jsonElement;
        if (jsonElement.isJsonObject()) {
            xpathObj = mMooDBContext.gson.fromJson(jsonElement, Map.class);
        } else if (jsonElement.isJsonArray()) {
            xpathObj = mMooDBContext.gson.fromJson(jsonElement, List.class);
        } else {
            logger.warn("performing xpath on primitive data type: {}", jsonElement);
        }
        JXPathContext ctx = JXPathContext.newContext(xpathObj);
        try {
            retval = mXPath.getValue(ctx);
            mResultObj = retval;
        } catch (JXPathNotFoundException e) {}
        return retval;
    }

    @Override
    public byte[] getData() {
        return mCursor.getData();
    }

    public Object getObj() {
        return mResultObj;
    }

    @Override
    public void close() {
        mCursor.close();
        mIsClosed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        if(!mIsClosed) {
            logger.warn("did not call close on cursor");
            close();
        }
        super.finalize();
    }
}
