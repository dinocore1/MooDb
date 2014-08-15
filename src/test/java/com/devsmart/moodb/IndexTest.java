package com.devsmart.moodb;


import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class IndexTest {

    private Map<String, Object> createWidget(String type, int value) {
        HashMap<String, Object> retval = new HashMap<String, Object>();
        retval.put("type", type);
        retval.put("value", value);
        retval.put("id", UUID.randomUUID());

        return retval;
    }

    private File mDBRoot;
    private MooDB mMooDB;

    @Before
    public void setupDB() throws Exception {
        mDBRoot = new File("data/test");
        if(mDBRoot.exists()){
            IOUtils.delete(mDBRoot);
        }
        mDBRoot.mkdirs();

        mMooDB = MooDB.openDatabase(mDBRoot);
    }

    @After
    public void closeDB() {
        mMooDB.close();
        mMooDB = null;
    }

    @Test
    public void testQuery() throws Exception {

        View view = mMooDB.addView("typeview", ".[type='car']");

        for(int i=0;i<5;i++){
            mMooDB.insert(createWidget("car", i));
        }
        for(int i=0;i<5;i++){
            mMooDB.insert(createWidget("plane", i));
        }
        for(int i=0;i<1000000;i++){
            mMooDB.insert(createWidget("train", i));
        }

        {
            Stopwatch stopwatch = Stopwatch.createStarted();
            XPathCursor cursor = view.query(".[type='car']");
            while (cursor.moveToNext()) {
                Object value = cursor.getObj();
                System.out.println("got value: " + value);
            }
            stopwatch.stop();
            cursor.close();
            System.out.println(String.format("%d view query took %s", 1000000, stopwatch));
        }

        {
            Stopwatch stopwatch = Stopwatch.createStarted();
            XPathCursor cursor = mMooDB.query(".[type='car']");
            while (cursor.moveToNext()) {
                Object value = cursor.getObj();
                System.out.println("got value: " + value);
            }
            stopwatch.stop();
            cursor.close();
            System.out.println(String.format("%d query took %s", 1000000, stopwatch));
        }




    }

    @Test
    public void testNewView() {
        ArrayList<Map> db = new ArrayList<Map>();
        for(int i=0;i<5;i++){
            mMooDB.insert(createWidget("car", i));
        }
        for(int i=0;i<5;i++){
            mMooDB.insert(createWidget("plane", i));
        }

        mMooDB.insert(new String[]{"this", "is", "my", "array"});

        //final String xpath = ".[type='car']/value";
        final String xpath = "type";
        mMooDB.addView("typeview", xpath);
    }
}
