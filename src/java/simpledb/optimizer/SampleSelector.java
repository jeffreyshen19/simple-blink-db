package simpledb.optimizer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.execution.Aggregate;
import simpledb.execution.OpIterator;
import simpledb.execution.Operator;
import simpledb.execution.Query;
import simpledb.execution.SeqScanSample;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
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
    public static int selectSample(QueryColumnSet qcs) {
        // still working on it rn - Victor
        Catalog catalog = Database.getCatalog();

        for (Iterator<Integer> iterator = catalog.tableIdIterator(); iterator.hasNext();) {
            int tableid = iterator.next();
            if (catalog.isSample(tableid)) { // Filter for samples - Jeffrey
                // if (sf.getColumnSet().getColumns().contains(qcs.getColumns())) {
                // return sf;
                // }
                // DbFile firstSample = sf.getSamples().get(0);
                // DbFileIterator sampleIterator = firstSample.iterator(null);
            }
        }

        throw new UnsupportedOperationException();
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
        } else { // Replace SeqScan
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
        double sd = Math.sqrt(variance);
        aggregate.close();

        // standard error = sd / sqrt(n) 
        return (int) Math.ceil(Math.pow(sd / errorTarget, 2));
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
     */
    public static int selectSampleSizeLatency(int sampleFamily, List<Integer> sampleSizes, OpIterator query,
            int latencyTarget) {
        // Run two queries on small samples (size n_1 and n_2), and calculate respective
        // latencies (y_1, y_2)
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
