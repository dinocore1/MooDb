package com.devsmart.moodb;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import javax.swing.*;
import java.util.concurrent.Future;

public class ParserTest {

    @Test
    public void test() throws Exception {
        String query = "[z=1 or q=1 and a=1 and b=1 or c=1]";

        MooDBLexer lexer = new MooDBLexer(new ANTLRInputStream(query));
        MooDBParser parser = new MooDBParser(new CommonTokenStream(lexer));
        MooDBParser.EvaluationContext tree = parser.evaluation();

        System.out.println(tree.toStringTree(parser));
        Future<JDialog> futureDialog = tree.inspect(parser);

        org.antlr.v4.runtime.misc.Utils.waitForClose(futureDialog.get());
    }
}
