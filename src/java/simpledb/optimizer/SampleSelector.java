package simpledb.optimizer;

import java.io.IOException;
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
import simpledb.execution.SeqScan;
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
     *
     * @param qcs
     * @return the tableid of a sample in the catalog
     */
    public int selectSample(QueryColumnSet qcs, OpIterator query) throws DbException, TransactionAbortedException{
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
                //int matchTuples = 0;

                OpIterator sampleQuery = modifyOperatorSampleFamily(tableid, query, totalTuples);
                // modify remove agg
                // modify make top agg
                //OpIterator noAggQuery = modifyOperatorRemoveAgg(sampleQuery);


                sampleQuery.open();
                runOperator(sampleQuery);
                int totalQueryTuples = sampleQuery.totalTuples();
                int numTuples = sampleQuery.numTuples();

                double ratio = numTuples/((double) totalQueryTuples);
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
     *
     * @param query
     */
    private static void runOperator(OpIterator query) {
        try {
            query.open();
            int i = 0;
            while (query.hasNext()) {
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
    private OpIterator modifyOperatorRemoveAgg(OpIterator query) {
        if (query instanceof Operator) {
            Operator operator = (Operator) query;
            OpIterator[] children = operator.getChildren();
            //return if no children - leaf node
            if (children.length == 0) return query;

            OpIterator[] newChildren = new OpIterator[children.length];
            int childIndex = 0;
            for (OpIterator child : children) {
                //check children to see if they are AGGREGATE
                //if so, then append child's children to current operator
                if (child instanceof Aggregate) {
                    OpIterator[] childchildren = ((Operator) child).getChildren();
                    for (OpIterator childChild : childchildren) {
                        newChildren[childIndex] = childChild;
                        modifyOperatorRemoveAgg(childChild);
                        childIndex++;
                }
                } else {
                    newChildren[childIndex] = child;
                    modifyOperatorRemoveAgg(child);
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
     *
     * @param sampleFamily
     * @param query
     * @param n
     * @return
     */
    public static OpIterator modifyOperatorSampleFamily(int sampleFamily, OpIterator query, int n) {
        if (query instanceof Operator) { // JOIN, FILTER, AGGREGATE
            Operator operator = (Operator) query;
            OpIterator[] children = operator.getChildren();
            OpIterator[] newChildren = new OpIterator[children.length];
            for (int i = 0; i < children.length; i++) {
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
     *
     * @param sampleFamily the tableid of the sample family
     * @param query        Query to execute, pointing to the original table not the
     *                     sample
     * @param n            Size of sample
     * @return latency in ms
     */
    public static int timeQueryOnSample(int sampleFamily, OpIterator query, int n) {
        OpIterator newQuery = modifyOperatorSampleFamily(sampleFamily, query, n);
        long startTime = System.currentTimeMillis();
        runOperator(newQuery);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime; // duration in ms

        return (int) duration;
    }

    /**
     * Given a sampleFamily and error target, return the estimated size of the
     * sample satisfying this target
     *
     * @param sampleFamily       the tableid of the sample family
     * @param sampleSize         the smallest sample size present in the sample family
     * @param tableSize          the number of tuples in the actual table
     * @param query              Query to execute
     * @param errorTarget        the target standard deviation
     * @return n, the number of rows to read from the sample
     * @throws TransactionAbortedException
     * @throws DbException
     */
    public static int selectSampleSizeError(int sampleFamily, int sampleSize, int tableSize, OpIterator query,
            double errorTarget) throws DbException, TransactionAbortedException {
        OpIterator newQuery = modifyOperatorSampleFamily(sampleFamily, query, sampleSize);

        // assuming that the top level is an aggregate here
        Aggregate aggregate = (Aggregate) newQuery;
        aggregate.open();


        // uses statistics from Table 2 in the BlinkDB paper
        double sampleVariance = aggregate.getSampleVariance();
        double selectednTups = aggregate.getNumTups();
        double variance, c;
        switch (aggregate.aggregateOp()) {
            case AVG:
                variance = sampleVariance / selectednTups;
                break;
            case COUNT:
                c = selectednTups / sampleSize;
                variance = Math.pow(sampleSize, 2) / selectednTups * c * (1 - c);
                break;
            case SUM:
                c = selectednTups / sampleSize;
                variance = Math.pow(sampleSize, 2) * (sampleVariance / selectednTups) * c * (1 - c);
                break;
            default:
                variance = 0; // should be unreachable

        }
        while (aggregate.hasNext()) {
        	System.out.println("Result: " + aggregate.next() + " sample size : "+ sampleSize);
        }
        aggregate.close();
//         c * 1/n = variance 
        System.out.println("variance" + Math.sqrt(variance));
        double constant = selectednTups * variance;
        return (int) Math.min(Math.ceil(constant / variance), tableSize); 
//
//        // standard error = sd / sqrt(n)
//        double error =  sd / Math.sqrt(selectednTups);
//        System.out.println("error: " + error + "sd" + sd);
//        return (int) Math.min(Math.ceil(Math.pow(sd / errorTarget, 2)), tableSize);
    }
    
    /**
     * Calculate error of query ran on sampleFamily
     *
     * @param sampleFamily       the tableid of the sample family
     * @param sampleSize         the smallest sample size present in the sample family
     * @param tableSize          the number of tuples in the actual table
     * @param query              Query to execute
     * @return n, the number of rows to read from the sample
     * @throws TransactionAbortedException
     * @throws DbException
     */
    public static double calculateError(int sampleFamily, int sampleSize, int tableSize, OpIterator query) throws DbException, TransactionAbortedException {
        OpIterator newQuery = modifyOperatorSampleFamily(sampleFamily, query, sampleSize);

        // assuming that the top level is an aggregate here
        Aggregate aggregate = (Aggregate) newQuery;
        aggregate.open();

        // uses statistics from Table 2 in the BlinkDB paper
        double sampleVariance = aggregate.getSampleVariance();
        double selectednTups = aggregate.getNumTups();
        System.out.println("selected ntups: " + selectednTups + " sample size: " + sampleSize);
        double variance, c;
        switch (aggregate.aggregateOp()) {
            case AVG:
                variance = sampleVariance / selectednTups;
                break;
            case COUNT:
                c = selectednTups / sampleSize;
                variance = Math.pow(sampleSize, 2) / selectednTups * c * (1 - c);
                break;
            case SUM:
                c = selectednTups / sampleSize;
                variance = Math.pow(sampleSize, 2) * (sampleVariance / selectednTups) * c * (1 - c);
                break;
            default:
                variance = 0; // should be unreachable

        }
        while (aggregate.hasNext()) {
        	System.out.println("Result: " + aggregate.next() + " sample size : "+ sampleSize);
        }
        aggregate.close();
        
//      c * 1/n = variance 
	    System.out.println("variance" + Math.sqrt(variance));
//	    double constant = selectednTups * variance;
	    return variance; 
    }

    /**
     * Given a sampleFamily and latency target, return the estimated size of the
     * sample satisfying this target
     *
     * @param sampleFamily  the tableid of the sample family
     * @param sampleSizes   the sampleSizes used to generate the sample
     * @param query         Query to execute
     * @param latencyTarget in ms
     * @return n, the number of rows to read from the sample
     * @throws IOException
     */

    public static int selectSampleSizeLatency(int sampleFamily, List<Integer> sampleSizes, OpIterator query, int latencyTarget) throws IOException {
        // Run two queries on small samples (size n_1 and n_2), and calculate respective latencies (y_1, y_2)
        // Solve linear equation to relate sample size n to latency y
        final int n1 = 10000;
        final int n2 = 30000;
        Database.getBufferPool().clearBufferPool();
        final int y2 = timeQueryOnSample(sampleFamily, query, n2);
        Database.getBufferPool().clearBufferPool();
        final int y1 = timeQueryOnSample(sampleFamily, query, n1);

        final double m = 1.0 * (y2 - y1) / (n2 - n1);
        final double b = y1 - m * n1;

        final int n = (int) Math.round((latencyTarget - b) / m);
        return n;
    }

}
