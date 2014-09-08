package com.devsmart.moodb;


import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
    public void testSimpleInsertGet() {
        ArrayList<Map> db = new ArrayList<Map>();
        for(int i=0;i<5;i++){
            mMooDB.insert(createWidget("car", i));
        }
        for(int i=0;i<5;i++){
            mMooDB.insert(createWidget("plane", i));
        }

        final String[] inserted = new String[]{"this", "is", "my", "array"};
        String id = mMooDB.insert(inserted);

        String[] returned = mMooDB.get(id, String[].class);

        assertTrue(Arrays.equals(returned, inserted));

    }

    @Test
    public void testQuery() throws Exception {

        mMooDB.loadIndex("type");

        for(int i=0;i<5;i++){
            mMooDB.insert(createWidget("car", i));
        }
        for(int i=0;i<5;i++){
            mMooDB.insert(createWidget("plane", i));
        }
        for(int i=0;i<500000;i++){
            mMooDB.insert(createWidget("train", i));
        }

        long queryTime;
        ArrayList<String> queryResult = new ArrayList<String>();
        {
            Stopwatch stopwatch = Stopwatch.createStarted();
            MooDBCursor cursor = mMooDB.query("[type='car']/id");
            int carCount = 0;
            while (cursor.moveToNext()) {
                carCount++;
                //Object value = cursor.getObj();
                //System.out.println("got value: " + value);
                //queryResult.add((String)value);
            }
            stopwatch.stop();
            cursor.close();
            System.out.println(String.format("query took %s", stopwatch));
            assertEquals(5, carCount);
            assertTrue(stopwatch.elapsed(TimeUnit.MILLISECONDS) < 100);

        }

    }


}
