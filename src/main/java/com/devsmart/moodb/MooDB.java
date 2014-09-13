package com.devsmart.moodb;


import com.devsmart.moodb.cursor.CursorBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sleepycat.je.*;

import java.io.File;
import java.io.IOException;
import java.util.TreeSet;
import java.util.UUID;

public class MooDB {

    private static final String DBNAME_OBJECTS = "objects";
    private static final String DBNAME_VIEWS = "views";
    private Environment mDBEnv;

    private final File mDBRoot;
    private Database mObjectsDB;
    private Database mViewsDB;
    private TreeSet<Index> mIndexes = new TreeSet<Index>();
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
        dbEnvConfig.setTransactional(true);
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

    public MooDBTransaction beginTransaction() {
        return new MooDBTransaction(mDBEnv.beginTransaction(null, null));
    }

    public String insert(Object obj) {
        String id = UUID.randomUUID().toString();

        if(put(id, obj)) {
            return id;
        } else {
            return null;
        }
    }

    public boolean put(String key, Object obj) {
        DatabaseEntry dbkey = new DatabaseEntry(Utils.toBytes(key));

        String jsonStr = gson.toJson(obj);
        DatabaseEntry dbvalue = new DatabaseEntry(Utils.toBytes(jsonStr));

        return mObjectsDB.put(null, dbkey, dbvalue) == OperationStatus.SUCCESS;
    }

    public byte[] get(String objectId) {
        DatabaseEntry key = new DatabaseEntry(Utils.toBytes(objectId));
        DatabaseEntry value = new DatabaseEntry();
        mObjectsDB.get(null, key, value, LockMode.DEFAULT);
        return value.getData();
    }

    public <T> T get(String objectId, Class<T> classType) {
        byte[] data = get(objectId);
        if(data != null) {
            String jsonStr = Utils.toString(data);
            return gson.fromJson(jsonStr, classType);
        } else {
            return null;
        }
    }

    public <T> T getOne(String xpath, Class<T> classType) {
        MooDBCursor cursor = query(xpath);
        try {
            if (cursor.moveToNext()) {
                return get(cursor.objectId(), classType);
            } else {
                return null;
            }
        } finally {
            cursor.close();
        }
    }

    public boolean delete(String objId) {
        DatabaseEntry key = new DatabaseEntry(Utils.toBytes(objId));
        return mObjectsDB.delete(null, key) == OperationStatus.SUCCESS;
    }


    private void loadViews() {
        Cursor cursor = mViewsDB.openCursor(null, null);
        try {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            while(cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS){
                String indexQuery = Utils.toString(key);
                loadIndex(indexQuery);
            }
        } finally {
            cursor.close();
        }
    }

    protected Index loadIndex(String indexQuery) {
        Index index = new Index(indexQuery);
        if(!mIndexes.contains(index)) {
            SecondaryConfig config = new SecondaryConfig();
            config.setAllowCreate(true);
            config.setAllowPopulate(true);
            config.setMultiKeyCreator(index);
            config.setSortedDuplicates(true);
            index.indexDB = mDBEnv.openSecondaryDatabase(null, indexQuery, mObjectsDB, config);

            mIndexes.add(index);
            return index;
        } else {
            return mIndexes.floor(index);
        }
    }

    public MooDBCursor query(String xpath) {
        MooDBCursor cursor = CursorBuilder.query(this, xpath);
        return cursor;
    }

    public Index getIndex(String indexQuery) {
        Index retval =  mIndexes.floor(new Index(indexQuery));
        if(retval.indexQuery.equals(indexQuery)){
            return retval;
        } else {
            return null;
        }
    }

    public void ensureIndex(String indexQuery) {
        loadIndex(indexQuery);
    }

    public void close() {
        for(Index index : mIndexes){
            index.indexDB.close();
        }
        mViewsDB.close();
        mObjectsDB.close();
        mDBEnv.close();
    }

    public Cursor openObjectsCursor(Transaction txn, CursorConfig config) {
        return mObjectsDB.openCursor(txn, config);
    }


}
