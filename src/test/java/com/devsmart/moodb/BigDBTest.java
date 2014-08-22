package com.devsmart.moodb;

import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class BigDBTest {

    private final File mDBRoot = new File("data/test");
    private MooDB mMooDB;

    @Before
    public void setupDB() throws Exception {
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

    private Map<String, Object> createWidget(String type, int value) {
        HashMap<String, Object> retval = new HashMap<String, Object>();
        retval.put("type", type);
        retval.put("value", value);
        retval.put("id", UUID.randomUUID());

        return retval;
    }

    private long timeGet(String id) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        mMooDB.get(id);
        stopwatch.stop();
        return stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    @Test
    public void testGetFromBigDB() {

        final String objId1 = mMooDB.insert(createWidget("thing", 0));
        final long getFromSmallDB = timeGet(objId1);


        for(int i=0;i<1000000;i++){
            mMooDB.insert(createWidget("birds", i));
        }
        final String objId2 = mMooDB.insert(createWidget("plane", 0));
        for(int i=0;i<1000000;i++){
            mMooDB.insert(createWidget("fish", i));
        }


        final long getFromBigDB = timeGet(objId2);
        final long diff = Math.abs(getFromBigDB - getFromSmallDB);
        String message = String.format("Time diff: %dms", diff);
        System.out.println(message);
        assertTrue(message, diff < 30);
    }
}
