package com.devsmart.moodb;

import com.sleepycat.je.*;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;

public class TestBerkeleyDB {

    @Test
    public void testDB() throws Exception {


        File baseDir = new File("data/env1");
        baseDir.mkdirs();

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        Environment myDbEnvironment = new Environment(baseDir, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database myDatabase = myDbEnvironment.openDatabase(null, "sampleDatabase", dbConfig);


        DatabaseEntry theKey = new DatabaseEntry("neat".getBytes("UTF-8"));
        DatabaseEntry theValue = new DatabaseEntry("value".getBytes("UTF-8"));
        myDatabase.put(null, theKey, theValue);

        DatabaseEntry data = new DatabaseEntry();
        assertTrue(myDatabase.get(null, theKey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS);
        assertEquals("value", new String(data.getData(), "UTF-8"));


        myDatabase.close();

    }

    @Test
    public void testIndex() throws Exception {

        File baseDir = new File("data/env1");
        baseDir.mkdirs();

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        Environment myDbEnvironment = new Environment(baseDir, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database myDatabase = myDbEnvironment.openDatabase(null, "primary", dbConfig);

        SecondaryConfig secondaryConfig = new SecondaryConfig();
        secondaryConfig.setAllowCreate(true);
        secondaryConfig.setKeyCreator(new SecondaryKeyCreator() {
            @Override
            public boolean createSecondaryKey(SecondaryDatabase secondary, DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
                return false;
            }
        });
        myDbEnvironment.openSecondaryDatabase(null, "secondary", myDatabase, secondaryConfig);
    }
}
