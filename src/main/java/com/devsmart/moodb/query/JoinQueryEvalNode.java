package com.devsmart.moodb.query;


import com.devsmart.moodb.MooDB;
import com.devsmart.moodb.MooDBCursor;
import com.google.common.base.Joiner;
import org.apache.commons.jxpath.ri.compiler.Expression;

public class JoinQueryEvalNode extends CombineQueryEvalNode {

    public JoinQueryEvalNode(Expression query) {

    }

    @Override
    public MooDBCursor createCursor(MooDB context) {
        MooDBCursor[] cursors = new MooDBCursor[mChildren.size()];
        for(int i=0;i<mChildren.size();i++){
            cursors[i] = mChildren.get(i).createCursor(context);
        }
        return new JoinCursor(context, cursors);
    }

    @Override
    public String toString() {
        return Joiner.on(" join ").join(mChildren);
    }
}
