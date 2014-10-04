package com.sleepycat.je.utilint;

import java.io.IOException;

public class TestHookAdapter<T> implements TestHook<T> {

    @Override
    public void hookSetup() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void doIOHook() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void doHook() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void doHook(T obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T getHookValue() {
        throw new UnsupportedOperationException();
    }
}
