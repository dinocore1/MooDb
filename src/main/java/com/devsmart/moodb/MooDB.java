package com.devsmart.moodb;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sleepycat.je.*;
import com.sleepycat.je.Cursor;
import org.apache.commons.jxpath.JXPathContext;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

public class MooDB {

    private static final String DBNAME_OBJECTS = "objects";
    private static final String DBNAME_VIEWS = "views";
    private Environment mDBEnv;


    private class ViewObj {
        View view;
        String name;
        SecondaryDatabase indexDB;
    }

    private final File mDBRoot;
    private Database mObjectsDB;
    private Database mViewsDB;
    private HashMap<String, ViewObj> mViews = new HashMap<String, ViewObj>();
    protected Gson gson = new GsonBuilder().create();

    public static MooDB openDatabase(File dbRoot) throws IOException {
        MooDB retval = new MooDB(dbRoot);
        retval.open();
        return retval;
    }

    private MooDB(File dbRoot) {
        mDBRoot = dbRoot;
    }

    private void open() throws IOException {
        mDBRoot.mkdirs();
        EnvironmentConfig dbEnvConfig = new EnvironmentConfig();
        dbEnvConfig.setAllowCreate(true);
        mDBEnv = new Environment(mDBRoot, dbEnvConfig);

        {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            mObjectsDB = mDBEnv.openDatabase(null, DBNAME_OBJECTS, dbConfig);
        }

        {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            mViewsDB = mDBEnv.openDatabase(null, DBNAME_VIEWS, dbConfig);
            loadViews();
        }
    }

    public String insert(Object obj) {
        String id = UUID.randomUUID().toString();

        if(insert(id, obj)) {
            return id;
        } else {
            return null;
        }
    }

    public boolean insert(String key, Object obj) {
        DatabaseEntry dbkey = new DatabaseEntry(Utils.toBytes(key));

        String jsonStr = gson.toJson(obj);
        DatabaseEntry dbvalue = new DatabaseEntry(Utils.toBytes(jsonStr));

        return mObjectsDB.put(null, dbkey, dbvalue) == OperationStatus.SUCCESS;
    }

    private void loadViews() {
        Cursor cursor = mViewsDB.openCursor(null, null);
        try {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            while(cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS){
                String name = Utils.toString(key);
                String xpath = Utils.toString(value);
                addView(name, xpath);
            }
        } finally {
            cursor.close();
        }
    }

    protected View addView(String name, String xpath) {
        ViewObj obj = new ViewObj();
        obj.name = name;
        obj.view = new View(this, JXPathContext.compile(xpath));

        SecondaryConfig config = new SecondaryConfig();
        config.setAllowCreate(true);
        config.setAllowPopulate(true);
        config.setKeyCreator(obj.view);
        config.setSortedDuplicates(true);
        obj.indexDB = mDBEnv.openSecondaryDatabase(null, name, mObjectsDB, config);
        obj.view.mIndexDB = obj.indexDB;

        mViews.put(name, obj);
        return obj.view;
    }

    public XPathCursor query(String xpath) {
        Cursor cursor = mObjectsDB.openCursor(null, null);
        XPathCursor retval = new XPathCursor(this, cursor, JXPathContext.compile(xpath));
        return retval;
    }

    public View getView(String viewName) {
        View retval = null;
        ViewObj obj = mViews.get(viewName);
        if(obj != null){
            retval = obj.view;
        }
        return retval;
    }

    public void close() {
        for(ViewObj index : mViews.values()){
            index.indexDB.close();
        }

        mViewsDB.close();
        mObjectsDB.close();
        mDBEnv.close();
    }


}
