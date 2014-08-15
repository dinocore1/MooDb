package com.devsmart.moodb;


import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ExecutionIndexTest {

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParams() {
        ArrayList<Object[]> retval = new ArrayList<Object[]>();

        retval.add(new Object[]{
                ".[type='car' and value='3']",
                new String[]{
                        "type",
                        "value",
                        ".[type='car']/id",
                        ".[type='plane]"
                },
                new int[][]{ {0,2}, {1} }
        });

        retval.add(new Object[]{
                ".[type='car' or value='3']",
                new String[]{
                        "type",
                        "value",
                        ".[type='car']/id",
                        ".[type='plane]"
                },
                new int[][]{ {0,2}, {1} }
        });

        retval.add(new Object[]{
                ".[(type='car' and value='3') or id='4']",
                new String[]{
                        "type",
                        "value",
                        ".[type='car']/id",
                        ".[type='plane]"
                },
                new int[][]{ {-1} }
        });

        return retval;
    }

    public ExecutionIndexTest(String query, String[] availableIndexes, int[][] requiredIndexes) {

    }
}
