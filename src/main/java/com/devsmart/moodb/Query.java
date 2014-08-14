package com.devsmart.moodb;

import org.apache.commons.jxpath.ri.compiler.Path;

import java.util.Collection;

public class Query {

    private final Path mXPath;
    private final Collection<Index> mIndexes;

    public Query(Path xpath, Collection<Index> indexes) {
        mXPath = xpath;
        mIndexes = indexes;
    }

    public void chooseIndexes() {

    }


}
