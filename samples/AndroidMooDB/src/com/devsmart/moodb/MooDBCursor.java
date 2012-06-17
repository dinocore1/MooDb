package com.devsmart.moodb;

public class MooDBCursor {

	private long mNativepointer;
	public String key;
	public String value;
	
	private native void cursorclose();
	
	public native boolean next();

	@Override
	protected void finalize() throws Throwable {
		cursorclose();
	}
	
	
}
