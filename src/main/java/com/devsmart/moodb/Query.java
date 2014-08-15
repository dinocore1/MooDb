package com.devsmart.moodb;

import org.apache.commons.jxpath.ri.compiler.Path;

import java.util.Collection;

public class Query {

    private final Path mXPath;
    private final Collection<View> mViews;

    public Query(Path xpath, Collection<View> views) {
        mXPath = xpath;
        mViews = views;
    }

    public void chooseIndexes() {

    }


}
