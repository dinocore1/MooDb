package com.devsmart.moodb;

public interface MooDBCursor {

    public static long BEFORE_FIRST = -1;
    public static long AFTER_LAST = -2;

    public boolean moveToNext();
    public boolean moveToPrevious();
    public byte[] getData();

}
