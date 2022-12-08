package simpledb.optimizer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.execution.Aggregate;
import simpledb.execution.OpIterator;
import simpledb.execution.Operator;
import simpledb.execution.Query;
import simpledb.execution.SeqScanSample;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.SampleDBFile;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class SampleSelector {
    
    /**
     * TODO: Victor 
     * Given a QueryColumnSet q_j, return the sample family to choose 
     * @param qcs
     * @return the tableid of a sample in the catalog
     */
    public int selectSample(QueryColumnSet qcs, OpIterator query) throws DbException, TransactionAbortedException{
        // still working on it rn - Victor
        Catalog catalog = Database.getCatalog();

        int minValidTableID = -1;
        int minValidSampleSize = Integer.MAX_VALUE;
        //check if QueryColumnSet fully contained in existing sample already
        for (Iterator<Integer> iterator = catalog.tableIdIterator(); iterator.hasNext(); ) {
            int tableid = iterator.next();
            if(catalog.isSample(tableid)) {
                SampleDBFile sample = (SampleDBFile) catalog.getDatabaseFile(tableid);
                //if the sample is the uniform sample, then skip
                if (!sample.isStratified()) continue;

                QueryColumnSet sampleQCS = sample.getStratifiedColumnSet();
                //get the smallest sample that fully contains it 
                if (sampleQCS.getColumns().contains(qcs.getColumns()) &&
                    sample.getSampleSizes().get(0) < minValidSampleSize) {
                        minValidSampleSize = sample.getSampleSizes().get(0);
                        minValidTableID = tableid;
                }
            }
        }

        if (minValidTableID != -1) {
            return minValidTableID;
        }

        Map<Integer, Double> tableidToRatio = new ConcurrentHashMap<Integer, Double>();

        // else- run query on each sample family and find one with largest selectivity
        for (Iterator<Integer> iterator = catalog.tableIdIterator(); iterator.hasNext(); ) {
            int tableid = iterator.next();
            if(catalog.isSample(tableid)) { // Filter for samples - Jeffrey 
                SampleDBFile sample = (SampleDBFile) catalog.getDatabaseFile(tableid);
                //get number of tuples in smallest sample in sampleFamily
                int totalTuples = sample.getSampleSizes().get(0);
                int matchTuples = 0;

                OpIterator sampleQuery = modifyOperatorSampleFamily(tableid, query, totalTuples);
                // modify remove agg
                // modify make top agg
                OpIterator countQuery = modifyOperatorCount(sampleQuery, false);

                countQuery.open();
                Tuple countTuple = countQuery.next();
                
                double ratio = matchTuples/((double) totalTuples);
                tableidToRatio.put(tableid, ratio);
            }
        }

        double maxSelectivity = Collections.max(tableidToRatio.values());
        for (int tableid : tableidToRatio.keySet()) {
            if (tableidToRatio.get(tableid) == maxSelectivity) return tableid;
        }


        
        throw new DbException("Should not have reached here");
    }
    
    /**
     * Runs an operator until completion
     * @param query
     */
    private static void runOperator(OpIterator query) {
        try {
            query.open();
            int i = 0;
            while(query.hasNext()) {
                query.next();
                i++;
            }
            query.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }     
    }

    /**
     * Modieifes an OpIterator to have its aggregation function be Count
     */
    private OpIterator modifyOperatorCount(OpIterator query, boolean aggEncountered) {
        if (query instanceof Operator) {
            Operator operator = (Operator) query;
            OpIterator[] children = operator.getChildren();
            OpIterator[] newChildren = new OpIterator[children.length];
            int childIndex = 0;
            for (OpIterator child : children) {
                if (child instanceof Aggregate) {
                    Aggregate currAgg = (Aggregate) query;
                    //want count of only very top query
                    if (aggEncountered) {
                        Aggregate newAgg = new Aggregate(currAgg.getChildren()[0], currAgg.aggregateField(), currAgg.groupField(), Op.COUNT);
                        modifyOperatorCount(newAgg, false);
                        newChildren[childIndex] = newAgg;
                    } else {
                        OpIterator[] childchildren = ((Operator) child).getChildren();


                    }
                } else {
                    modifyOperatorCount(child, aggEncountered);
                    newChildren[childIndex] = child;
                }
                childIndex++;
            }
            operator.setChildren(newChildren);
            return operator;
        } else {
            return query;
        }
    }
    
    
    /**
     * Modifies an OpIterator to point to SeqScanSample instead of SeqScan
     * @param sampleFamily
     * @param query
     * @param n
     * @return
     */
    public static OpIterator modifyOperatorSampleFamily(int sampleFamily, OpIterator query, int n) {
        if(query instanceof Operator) { // JOIN, FILTER, AGGREGATE
            Operator operator = (Operator) query;
            OpIterator[] children = operator.getChildren();
            OpIterator[] newChildren = new OpIterator[children.length];
            for(int i = 0; i < children.length; i++) {
                newChildren[i] = modifyOperatorSampleFamily(sampleFamily, children[i], n);
            }
            operator.setChildren(newChildren);
            return operator;
        }
        else { // Replace SeqScan 
            return new SeqScanSample(null, sampleFamily, n);
        }
    }
    
    /**
     * Return the latency of running a query on a sample of size n
     * @param sampleFamily the tableid of the sample family
     * @param query Query to execute, pointing to the original table not the sample 
     * @param n Size of sample
     * @return latency in ms 
     */
    public static int timeQueryOnSample(int sampleFamily, OpIterator query, int n) {
        OpIterator newQuery = modifyOperatorSampleFamily(sampleFamily, query, n);
        long startTime = System.currentTimeMillis();
        runOperator(newQuery);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;  // duration in ms
        
        return (int) duration;
    }
    
    /**
     * TODO: Yun
     * Given a sampleFamily and error target, return the estimated size of the sample satisfying this target
     * @param sampleFamily the tableid of the sample family
     * @param query Query to execute
     * @param errorTarget the target standard deviation 
     * @return n, the number of rows to read from the sample
     */
    public static int selectSampleSizeError(int sampleFamily, OpIterator query, double errorTarget) {
        return 0;
        //TODO: yun, when writing this, you can assume that the functions to run a query actually work
    }
    
    /**
     * Given a sampleFamily and latency target, return the estimated size of the sample satisfying this target
     * @param sampleFamily the tableid of the sample family
     * @param sampleSizes the sampleSizes used to generate the sample
     * @param query Query to execute
     * @param latencyTarget in ms
     * @return n, the number of rows to read from the sample
     */
    public static int selectSampleSizeLatency(int sampleFamily, List<Integer> sampleSizes, OpIterator query, int latencyTarget) {
        // Run two queries on small samples (size n_1 and n_2), and calculate respective latencies (y_1, y_2) 
        // Solve linear equation to relate sample size n to latency y 
        final int n1 = sampleSizes.get(0); 
        final int n2 = sampleSizes.get(1); 
        final int y1 = timeQueryOnSample(sampleFamily, query, n1);
        final int y2 = timeQueryOnSample(sampleFamily, query, n2);

        final double m = 1.0 * (y2 - y1) / (n2 - n1);
        final double b = y1 - m * n1;
        
        final int n = (int) Math.round((latencyTarget - b) / m);
        return n;
    }
    
}
