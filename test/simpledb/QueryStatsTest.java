package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.Map;

import simpledb.optimizer.QueryColumnSet;
import simpledb.optimizer.QueryStats;

import org.junit.Test;

public class QueryStatsTest {
    
    @Test public void queryColumnSet() {
        QueryColumnSet qcs1 = new QueryColumnSet(0, 1, 2);
        QueryColumnSet qcs2 = new QueryColumnSet(2, 1, 0);
        QueryColumnSet qcs3 = new QueryColumnSet(0, 1);
        
        assertEquals(qcs1, qcs2);
        assertNotEquals(qcs1, qcs3);
        assertEquals(qcs1.hashCode(), qcs2.hashCode());
    }
    
    @Test public void getProbability() {
        ArrayList<QueryColumnSet> pastQueries = new ArrayList<>();
        
        pastQueries.add(new QueryColumnSet(0, 1, 2));
        pastQueries.add(new QueryColumnSet(2, 1, 0));
        pastQueries.add(new QueryColumnSet(0, 2, 3));
        
        QueryStats qs = new QueryStats(pastQueries);
        
        assertEquals(qs.getProbability(new QueryColumnSet(0, 1, 2)), 2.0f/3, 0.001);
        assertEquals(qs.getProbability(new QueryColumnSet(0, 3, 2)), 1.0f/3, 0.001);
        assertEquals(qs.getProbability(new QueryColumnSet(0, 3)), 0.0f, 0.001);
    }
    
    @Test public void getSortedProbabilities() {
        ArrayList<QueryColumnSet> pastQueries = new ArrayList<>();
        
        pastQueries.add(new QueryColumnSet(0, 1, 2));
        pastQueries.add(new QueryColumnSet(2, 1, 0));
        pastQueries.add(new QueryColumnSet(0, 2, 3));
        
        QueryStats qs = new QueryStats(pastQueries);
        
        Map<QueryColumnSet, Float> map = qs.getSortedProbabilities();
        QueryColumnSet[] expectedKeys = {new QueryColumnSet(0, 1, 2), new QueryColumnSet(0, 3, 2)};
        float[] expectedValues = {2.0f/3, 1.0f/3};
        
        int i = 0;
        for(QueryColumnSet qcs : map.keySet()) {
            assertEquals(expectedKeys[i], qcs);
            assertEquals(expectedValues[i], (float) map.get(qcs), 0.001);
            i++;
        }
    }
}
