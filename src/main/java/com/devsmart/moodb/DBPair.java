package com.devsmart.moodb;


public class DBPair<T> {
    public final String key;
    public final T obj;

    public DBPair(String key, T obj) {
        this.key = key;
        this.obj = obj;
    }
}
