package com.devsmart.moodb;

import java.io.File;

import android.app.Activity;
import android.os.Bundle;

public class AndroidMooDBActivity extends Activity {
    private MooDBWrapper mWrapper;

	/** Called when the activity is first created. */
    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.main);
        
        File db = getFileStreamPath("testmoodb.db");
        mWrapper = new MooDBWrapper(db.getAbsolutePath());
        
        String newkey = mWrapper.putObject(null, "{}");
        
    }
}