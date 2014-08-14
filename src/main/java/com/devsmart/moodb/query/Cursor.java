package com.devsmart.moodb.query;

public interface Cursor {

    public static long BEFORE_FIRST = -1;
    public static long AFTER_LAST = -2;

    public boolean moveToNext();
    public boolean moveToPrevious();
    public byte[] getData();

}
