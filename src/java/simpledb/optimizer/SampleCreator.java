package simpledb.optimizer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.execution.OpIterator;

public class SampleCreator {
    
    public static final int IO_COST = 71;

    /**
     * Return a map, where keys are tableIds and values are a list of QueryColumnSets, representing
     * the stratified samples to create 
     * @param queryWorkload map of tableids to list of past queries
     * @return 
     */
    public static Map<Integer, List<QueryColumnSet>> getStratifiedSamplesToCreate(Map<Integer, List<OpIterator>> queryWorkload, int k){
       Map<Integer, List<QueryColumnSet>> samplesToCreate = new HashMap<>();
       Catalog catalog = Database.getCatalog();
       
       // Consider all tables that are not samples
       for(Iterator<Integer> iterator = catalog.tableIdIterator(); iterator.hasNext();) {
           int tableid = iterator.next();
           if(!catalog.isSample(tableid)) {
               
               // Generate query distribution
               List<QueryColumnSet> pastQueries = new ArrayList<>();
               for(OpIterator query : queryWorkload.get(tableid)) {
                   pastQueries.add(new QueryColumnSet(query));
               }
               QueryStats qs = new QueryStats(pastQueries);
               
               // Pick k most skewed columns that are in the query workload w/ p > 0.05
               TableStats tableStats = new TableStats(tableid, IO_COST);
               int numColumns = catalog.getTupleDesc(tableid).numFields();
               Map<Integer, Double> columnToSkew = new HashMap<>();

               for(int column = 0; column < numColumns; column++) {
                   double skew = tableStats.getSkewForColumn(column);;
                   columnToSkew.put(column, Math.abs(skew));
               }
               
               List<Entry<Integer, Double>> list = new ArrayList<>(columnToSkew.entrySet());
               list.sort(Entry.<Integer, Double>comparingByValue().reversed());

               int i = 0;
               List<QueryColumnSet> qcses = new ArrayList<>();
               for (Entry<Integer, Double> entry : list) {
                   if(i >= k) break;
                   int column = entry.getKey();
                   QueryColumnSet qcs = new QueryColumnSet(column);
                   if(qs.getProbability(qcs) > 0) qcses.add(qcs);
                   i++;
               }
               samplesToCreate.put(tableid, qcses);
               
               
           }
       }
       
       return samplesToCreate;
    }
}
