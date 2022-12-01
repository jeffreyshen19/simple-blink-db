package simpledb.optimizer;

import java.util.List;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.execution.OpIterator;
import simpledb.execution.Operator;
import simpledb.execution.Query;
import simpledb.storage.SampleFamily;
import simpledb.transaction.TransactionAbortedException;

public class SampleSelector {
    
    /**
     * TODO: Victor 
     * Given a QueryColumnSet q_j, return the sample family to choose 
     * @param qcs
     * @return SampleFamily
     */
    public SampleFamily selectSample(QueryColumnSet qcs) {
        List<SampleFamily> sampleFamilies = Database.getSampleFamilies();
        throw new UnsupportedOperationException();
    }
    
    /**
     * Runs an operator until completion
     * @param query
     */
    private void runOperator(OpIterator query) {
        try {
            query.open();
            while(query.hasNext()) {
                query.next();
            }
            query.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }     
    }
    
    /**
     * Return the latency of running a query on a sample of size n
     * @param sampleFamily
     * @param query Query to execute
     * @param n Size of sample
     * @return latency in ms 
     */
    private int timeQueryOnSample(SampleFamily sampleFamily, OpIterator query, int n) {
        // TODO: modify query so that SeqScan references sampleFamily of size n
        long startTime = System.nanoTime();
        runOperator(query);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;  // duration in ms
        
        return (int) duration;
    }
    
    /**
     * TODO: Yun
     * Given a sampleFamily and error target, return the estimated size of the sample satisfying this target
     * @param sampleFamily
     * @param query Query to execute
     * @param errorTarget the target standard deviation 
     * @return n, the number of rows to read from the sample
     */
    public int selectSampleSizeError(SampleFamily sampleFamily, OpIterator query, double errorTarget) {
        return 0;
    }
    
    /**
     * Given a sampleFamily and latency target, return the estimated size of the sample satisfying this target
     * @param sampleFamily
     * @param query Query to execute
     * @param latencyTarget
     * @return n, the number of rows to read from the sample
     */
    public int selectSampleSizeLatency(SampleFamily sampleFamily, OpIterator query, double latencyTarget) {
        // Run two queries on small samples (size n_1 and n_2), and calculate respective latencies (y_1, y_2) 
        // Solve linear equation to relate sample size n to latency y 
        final int n1 = 100; // TODO: test if we need to adjust these values 
        final int n2 = 200; 
        final int y1 = timeQueryOnSample(sampleFamily, query, n1);
        final int y2 = timeQueryOnSample(sampleFamily, query, n2);
        
        final double m = 1.0 * (y2 - y1) / (n2 - n1);
        final double b = y1 - m * n1;
        
        final int n = (int) Math.round((latencyTarget - b) / m);
        return n;
    }
    
}
