package com.devsmart.moodb;

import java.io.File;

import android.app.Activity;
import android.os.Bundle;
import android.util.Log;

public class AndroidMooDBActivity extends Activity {
    private MooDBWrapper mWrapper;

	/** Called when the activity is first created. */
    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.main);
        
        File db = getFileStreamPath("testmoodb.db");
        mWrapper = new MooDBWrapper(db.getAbsolutePath());
        
        String newkey = mWrapper.putObject(null, "{firstname: \"Paul\", lastname: \"Soucy\", age:27 }");
        mWrapper.putObject(null, "{ firstname: \"Melanie\", lastname: \"Silverman\", age:25, eyecolor: \"brown\" }");
        
        mWrapper.putView("{name: \"people\", map: function(obj) { if(obj.eyecolor){emit(\"eyecolor\", obj.eyecolor);} " + 
        		"if(obj.age){ emit(\"age\", obj.age);} } }");
        
        MooDBCursor cursor = mWrapper.query("{view: \"m_people\", filter: \"age <> 25\" }");
        while(cursor.next()){
        	Log.i(AndroidMooDBActivity.class.getName(), "obj: " + cursor.key + " : " + cursor.value);
        }
        
    }
}