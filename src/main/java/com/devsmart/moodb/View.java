package com.devsmart.moodb;


import com.devsmart.moodb.query.IndexCursor;
import com.devsmart.moodb.query.IterateIndexQueryEvalNode;
import com.google.gson.JsonElement;
import com.sleepycat.bind.tuple.SortedDoubleBinding;
import com.sleepycat.je.*;
import org.apache.commons.jxpath.CompiledExpression;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.JXPathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class View implements SecondaryKeyCreator {

    Logger logger = LoggerFactory.getLogger(View.class);

    public final CompiledExpression mXPath;
    private final MooDB mMooDBContext;
    public SecondaryDatabase mIndexDB;

    protected View(MooDB mooDB, CompiledExpression xpath) {
        mMooDBContext = mooDB;
        mXPath = xpath;
    }

    @Override
    public boolean createSecondaryKey(SecondaryDatabase secondary, DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
        JsonElement obj = Utils.toJsonElement(data, mMooDBContext.gson);
        Object xpathObj;
        if(obj.isJsonObject()){
            xpathObj = mMooDBContext.gson.fromJson(obj, Map.class);
        } else if(obj.isJsonArray()) {
            xpathObj = mMooDBContext.gson.fromJson(obj, List.class);
        } else {
            xpathObj = Utils.toString(data.getData());
        }
        JXPathContext ctx = JXPathContext.newContext(xpathObj);
        try {
            Object resultObj = mXPath.getValue(ctx);
            if(resultObj instanceof Number) {
                double value = ((Number)resultObj).doubleValue();
                SortedDoubleBinding.doubleToEntry(value, result);
                logger.debug("Index {}: emit key: {}", mXPath, resultObj);
            } else if(resultObj instanceof String) {
                result.setData(Utils.toBytes((String)resultObj));
                logger.debug("Index {}: emit key: {}", mXPath, resultObj);
            } else {
                String str = mMooDBContext.gson.toJson(resultObj);
                result.setData(Utils.toBytes(str));
                logger.debug("Index {}: emit key: {}", mXPath, str);
            }

            return true;
        } catch(JXPathNotFoundException e) {
            return false;
        }
    }

    public XPathCursor query(String xpath) {
        SecondaryCursor cursor = mIndexDB.openCursor(null, null);
        XPathCursor retval = new XPathCursor(mMooDBContext, new IndexCursor(cursor), JXPathContext.compile(xpath));
        return retval;
    }

}
