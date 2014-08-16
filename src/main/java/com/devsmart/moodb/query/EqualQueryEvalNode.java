package com.devsmart.moodb.query;


import com.devsmart.moodb.MooDB;
import com.devsmart.moodb.MooDBCursor;
import com.devsmart.moodb.Utils;
import com.devsmart.moodb.View;
import com.sleepycat.bind.tuple.FloatBinding;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.SortedDoubleBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryCursor;
import org.apache.commons.jxpath.ri.compiler.LocationPath;

public class EqualQueryEvalNode extends QueryEvalNode {

    private final LocationPath mIndex;
    private final Object mValue;

    public EqualQueryEvalNode(LocationPath index, Object value) {
        mIndex = index;
        mValue = value;
    }

    @Override
    public String toString() {
        return String.format("[i:%s = %s]", mIndex, mValue);
    }

    @Override
    public MooDBCursor createCursor(MooDB context) {
        View index = context.getIndex(mIndex.toString());
        SecondaryCursor cursor = index.mIndexDB.openCursor(null, null);

        DatabaseEntry key = new DatabaseEntry();
        if(mValue instanceof Number) {
            double value = ((Number)mValue).doubleValue();
            SortedDoubleBinding.doubleToEntry(value, key);
        } else if(mValue instanceof String){
            key.setData(Utils.toBytes((String)mValue));
        }
        return new IndexEqualCursor(cursor, key);
    }
}
