package com.devsmart.moodb;


import com.google.gson.JsonElement;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import org.apache.commons.jxpath.CompiledExpression;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.JXPathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class Index implements SecondaryKeyCreator {

    Logger logger = LoggerFactory.getLogger(Index.class);

    public final CompiledExpression mXPath;
    private final MooDB mMooDBContext;

    protected Index(MooDB mooDB, CompiledExpression xpath) {
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
            String str = mMooDBContext.gson.toJson(resultObj);
            result.setData(Utils.toBytes(str));
            logger.debug("Index {}: emit key: {}", mXPath, str);
            return true;
        } catch(JXPathNotFoundException e) {
            return false;
        }
    }
}
