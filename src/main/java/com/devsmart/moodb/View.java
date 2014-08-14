package com.devsmart.moodb;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.sleepycat.je.*;
import org.apache.commons.jxpath.CompiledExpression;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.JXPathIntrospector;
import org.apache.commons.jxpath.JXPathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class View implements SecondaryKeyCreator {

    Logger logger = LoggerFactory.getLogger(View.class);


    private final SecondaryDatabase mIndexDB;
    private final CompiledExpression mXPath;
    private final MooDB mMooDBContext;

    protected View(MooDB mooDB, String xpath, Database objectsDB) {
        mMooDBContext = mooDB;
        mXPath = JXPathContext.compile(xpath);

        SecondaryConfig config = new SecondaryConfig();
        config.setAllowCreate(true);
        config.setAllowPopulate(true);
        config.setKeyCreator(this);
        mIndexDB = objectsDB.getEnvironment().openSecondaryDatabase(null, xpath, objectsDB, config);
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
            return true;
        } catch(JXPathNotFoundException e) {
            return false;
        }
    }
}
