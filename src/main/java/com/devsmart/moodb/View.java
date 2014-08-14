package com.devsmart.moodb;


import com.google.gson.JsonElement;
import com.sleepycat.je.*;
import org.apache.commons.jxpath.CompiledExpression;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.JXPathNotFoundException;

import java.util.Map;

public class View implements SecondaryKeyCreator {

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
        //JsonElement obj = Utils.toJsonElement(data, mMooDBContext.gson);
        Map obj = mMooDBContext.gson.fromJson(Utils.toString(data.getData()), Map.class);
        JXPathContext ctx = JXPathContext.newContext(obj);
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
