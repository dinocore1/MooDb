package com.devsmart.moodb;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sleepycat.je.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.jxpath.ri.Parser;
import org.apache.commons.jxpath.ri.compiler.Expression;
import org.apache.commons.jxpath.ri.compiler.TreeCompiler;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class MooDB {

    private static final String DBNAME_OBJECTS = "objects";
    private static final String DBNAME_VIEWS = "views";

    private final File mDBRoot;
    private Database mObjectsDB;
    private Database mViewsDB;
    private HashMap<String, View> mViews = new HashMap<String, View>();
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
            loadViews();
        }
    }

    public String insert(Object obj) {
        String str = gson.toJson(obj);
        byte[] objdata = Utils.toBytes(str);
        byte[] keydata = Utils.getSha1Hash().digest(objdata);

        DatabaseEntry key = new DatabaseEntry(keydata);
        DatabaseEntry value = new DatabaseEntry(objdata);

        if(mObjectsDB.put(null, key, value) == OperationStatus.SUCCESS){
            return Utils.toString(new Base64().encode(keydata));
        } else {
            return null;
        }
    }

    private void loadViews() {
        Cursor cursor = mViewsDB.openCursor(null, null);
        try {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            while(cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS){
                String xpath = Utils.toString(key);
                addView(xpath);
            }
        } finally {
            cursor.close();
        }
    }

    //private static final TreeCompiler COMPILER = new TreeCompiler();
    protected void addView(String xpath) {
        View view = new View(this, xpath, mObjectsDB);
        mViews.put(xpath, view);
        //Expression expression = (Expression)Parser.parseExpression(xpath, COMPILER);

    }

    public void close() {

    }
}
