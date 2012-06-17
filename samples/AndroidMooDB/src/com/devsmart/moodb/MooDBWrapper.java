package com.devsmart.moodb;

public class MooDBWrapper {
	
	static {
		System.loadLibrary("moodbwrapper");
	} 
	
	private long mNativepointer;
	
	private native int moodbopen(String filepath);
	private native int moodbclose();
	
	public MooDBWrapper(String filepath){
		moodbopen(filepath);
	}
	
	public native String putObject(String key, String data);
	
	public native void putView(String viewspec);
	
	public native MooDBCursor query(String query);

}
