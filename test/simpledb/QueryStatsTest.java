package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import simpledb.optimizer.QueryColumnSet;
import simpledb.optimizer.QueryStats;

import org.junit.Test;

public class QueryStatsTest {
    @Test public void queryColumnSet() {
        QueryColumnSet qcs1 = new QueryColumnSet("a", "b", "c");
        QueryColumnSet qcs2 = new QueryColumnSet("c", "b", "a");
        QueryColumnSet qcs3 = new QueryColumnSet("a", "b");
        
        assertEquals(qcs1, qcs2);
        assertNotEquals(qcs1, qcs3);
        assertEquals(qcs1.hashCode(), qcs2.hashCode());
    }
    
    @Test public void getProbability() {
        ArrayList<QueryColumnSet> pastQueries = new ArrayList<>();
        
        pastQueries.add(new QueryColumnSet("a", "b", "c"));
        pastQueries.add(new QueryColumnSet("b", "c", "a"));
        pastQueries.add(new QueryColumnSet("a", "c", "d"));
        
        QueryStats qs = new QueryStats(pastQueries);
        
        assertEquals(qs.getProbability(new QueryColumnSet("a", "b", "c")), 2.0f/3, 0.001);
        assertEquals(qs.getProbability(new QueryColumnSet("a", "d", "c")), 1.0f/3, 0.001);
        assertEquals(qs.getProbability(new QueryColumnSet("a", "d")), 0.0f, 0.001);
    }
}
