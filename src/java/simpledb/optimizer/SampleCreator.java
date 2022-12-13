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
import simpledb.storage.BufferPool;
import simpledb.storage.HeapFile;

public class SampleCreator {
    
    public static final int IO_COST = 71;

    /**
     * Return a list of QueryColumnSets representing the stratified samples to create for a table
     * @param queryWorkload list of past queries
     * @return 
     */
    public static List<QueryColumnSet> getStratifiedSamplesToCreate(int tableid, List<OpIterator> queryWorkload, int storageCap){
       List<QueryColumnSet> samplesToCreate = new ArrayList<>();
       Catalog catalog = Database.getCatalog();
       
       // Calculate how many samples we can generate
       final int DB_SIZE = ((HeapFile) catalog.getDatabaseFile(tableid)).numPages() * BufferPool.getPageSize(); // Bytes in original DB 
       final int SAMPLE_SIZE = (int) (DB_SIZE * 0.02); // Sample should be 2% of DB
       final int K = storageCap / SAMPLE_SIZE; // K = number of samples we can generate 
      
       // Generate query distribution
       List<QueryColumnSet> pastQueries = new ArrayList<>();
       for(OpIterator query : queryWorkload) pastQueries.add(new QueryColumnSet(query));
       QueryStats qs = new QueryStats(pastQueries);
       
       // Pick k most skewed columns that are in the query workload w/ p > 0.05
       TableStats tableStats = new TableStats(tableid, IO_COST);
       int numColumns = catalog.getTupleDesc(tableid).numFields();
       Map<QueryColumnSet, Double> columnToSkew = new HashMap<>();

       for(int column = 0; column < numColumns; column++) {
           double skew = tableStats.getSkewForColumn(column);
           QueryColumnSet qcs = new QueryColumnSet(column);
           
           if(qs.getProbability(qcs) > 0.05) columnToSkew.put(qcs, Math.abs(skew));
       }
       
       List<Entry<QueryColumnSet, Double>> list = new ArrayList<>(columnToSkew.entrySet());
       list.sort(Entry.<QueryColumnSet, Double>comparingByValue().reversed());

       int i = 0;
       for (Entry<QueryColumnSet, Double> entry : list) {
           if(i >= K) break;
           QueryColumnSet qcs = entry.getKey();
           samplesToCreate.add(qcs);
           i++;
       }
       
       return samplesToCreate;
    }
}
