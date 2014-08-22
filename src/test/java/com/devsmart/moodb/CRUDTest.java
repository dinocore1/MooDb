package com.devsmart.moodb;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;

public class CRUDTest {

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

    static class MyPOJO {
        public String firstName;
        public String lastName;
        public int age;

        @Override
        public boolean equals(Object obj) {
            boolean retval = false;
            if(obj instanceof MyPOJO){
                MyPOJO other = (MyPOJO) obj;
                retval = firstName.equals(other.firstName) &&
                        lastName.equals(other.lastName) &&
                        age == other.age;
            }
            return retval;
        }
    }

    @Test
    public void testInsert() {
        MyPOJO obj1 = new MyPOJO();
        obj1.firstName = "Ben";
        obj1.lastName = "Rodgers";
        obj1.age = 27;

        final String objId = mMooDB.insert(obj1);

        assertNotNull(obj1);

        MyPOJO returnedObj = mMooDB.get(objId, MyPOJO.class);
        assertNotNull(returnedObj);
        assertTrue(obj1.equals(returnedObj));
    }

    @Test
    public void testUpdate() {
        MyPOJO obj1 = new MyPOJO();
        {
            obj1.firstName = "Ben";
            obj1.lastName = "Rodgers";
            obj1.age = 27;
        }

        final String objId = mMooDB.insert(obj1);

        assertNotNull(obj1);

        MyPOJO obj2 = new MyPOJO();
        {
            obj2.firstName = "Ben";
            obj2.lastName = "Rodgers";
            obj2.age = 28;
        }

        mMooDB.put(objId, obj2);
        MyPOJO returnedObj = mMooDB.get(objId, MyPOJO.class);
        assertNotNull(returnedObj);
        assertTrue(obj2.equals(returnedObj));

    }

    @Test
    public void testDelete() {
        MyPOJO obj1 = new MyPOJO();
        {
            obj1.firstName = "Ben";
            obj1.lastName = "Rodgers";
            obj1.age = 27;
        }

        final String objId = mMooDB.insert(obj1);
        assertTrue(mMooDB.delete(objId));

        MyPOJO returnedObj = mMooDB.get(objId, MyPOJO.class);
        assertNull(returnedObj);
    }
}
