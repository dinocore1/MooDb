package com.devsmart.moodb;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sleepycat.je.*;
import org.apache.commons.jxpath.JXPathContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MooDB {

    private static final String DBNAME_OBJECTS = "objects";
    private static final String DBNAME_VIEWS = "views";

    private class IndexObj {
        Index view;
        SecondaryDatabase indexDB;
    }

    private final File mDBRoot;
    private Database mObjectsDB;
    private Database mViewsDB;
    private List<IndexObj> mViews = new ArrayList<IndexObj>();
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
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        Environment dbEnv = new Environment(mDBRoot, envConfig);

        {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            mObjectsDB = dbEnv.openDatabase(null, DBNAME_OBJECTS, dbConfig);
        }

        {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            mViewsDB = dbEnv.openDatabase(null, DBNAME_VIEWS, dbConfig);
            loadIndexes();
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

    private void loadIndexes() {
        Cursor cursor = mViewsDB.openCursor(null, null);
        try {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            while(cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS){
                String xpath = Utils.toString(key);
                addIndex(xpath);
            }
        } finally {
            cursor.close();
        }
    }

    protected void addIndex(String xpath) {
        IndexObj obj = new IndexObj();
        obj.view = new Index(this, JXPathContext.compile(xpath));


        SecondaryConfig config = new SecondaryConfig();
        config.setAllowCreate(true);
        config.setAllowPopulate(true);
        config.setKeyCreator(obj.view);
        config.setSortedDuplicates(true);
        obj.indexDB = mObjectsDB.getEnvironment().openSecondaryDatabase(null, "xpath" + xpath, mObjectsDB, config);

        mViews.add(obj);
    }

    public void close() {
        for(IndexObj index : mViews){
            index.indexDB.close();
        }

        mViewsDB.close();

        mObjectsDB.close();
    }


}
