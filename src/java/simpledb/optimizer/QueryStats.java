package simpledb.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class QueryStats {
    
    // Store query workload distribution
    private int totalQueries;
    private Map<QueryColumnSet, Float> qcsCounts;
    
    public QueryStats(List<QueryColumnSet> pastQueries) {
        totalQueries = pastQueries.size();
        qcsCounts = new HashMap<>();
        
        for(int i = 0; i < pastQueries.size(); i++) {
            QueryColumnSet qcs = pastQueries.get(i);
            if(!qcsCounts.containsKey(qcs)) qcsCounts.put(qcs, 1.0f);
            else qcsCounts.put(qcs, qcsCounts.get(qcs) + 1);
        }
        
        for(QueryColumnSet key : qcsCounts.keySet()) {
            qcsCounts.put(key, qcsCounts.get(key) / totalQueries);
        }
    }
    
    /**
     * Get the frequency of a QCS based off past queries 
     * @param qcs
     * @return frequency of queries with QCS in past queries
     */
    public float getProbability(QueryColumnSet qcs) {
        if(!qcsCounts.containsKey(qcs)) return 0;
        return qcsCounts.get(qcs);
    }
    
    /**
     * Returns a LinkedHashMap of the qcsCounts, sorted in descending probability order
     * @return
     */
    public LinkedHashMap<QueryColumnSet, Float> getSortedProbabilities() {
        List<Entry<QueryColumnSet, Float>> list = new ArrayList<>(qcsCounts.entrySet());
        list.sort(Entry.<QueryColumnSet, Float>comparingByValue().reversed());

        LinkedHashMap<QueryColumnSet, Float> result = new LinkedHashMap<>();
        for (Entry<QueryColumnSet, Float> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

}
