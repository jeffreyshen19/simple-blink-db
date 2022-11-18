package simpledb.optimizer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryStats {
    
    // Store query workload distribution
    private int totalQueries;
    private Map<QueryColumnSet, Integer> qcsCounts;
    
    public QueryStats(List<QueryColumnSet> pastQueries) {
        totalQueries = pastQueries.size();
        qcsCounts = new HashMap<>();
        
        for(int i = 0; i < pastQueries.size(); i++) {
            QueryColumnSet qcs = pastQueries.get(i);
            if(!qcsCounts.containsKey(qcs)) qcsCounts.put(qcs, 1);
            else qcsCounts.put(qcs, qcsCounts.get(qcs) + 1);
        }
    }
    
    /**
     * Get the frequency of a QCS based off past queries 
     * @param qcs
     * @return frequency of queries with QCS in past queries
     */
    public float getProbability(QueryColumnSet qcs) {
        if(!qcsCounts.containsKey(qcs)) return 0;
        return 1.0f * qcsCounts.get(qcs) / totalQueries;
    }

}
