package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private Map<Integer, IntHistogram> intHistograms; // maps integer field i to histogram
    private Map<Integer, StringHistogram> stringHistograms; // maps string field i to histogram
    private int totalTuples = 0;
    private int ioCostPerPage;
    private HeapFile dbfile;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        
        this.ioCostPerPage = ioCostPerPage;
        this.intHistograms = new HashMap<>();
        this.stringHistograms = new HashMap<>();
        
        dbfile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);//.iterator(new TransactionId());
        TupleDesc td = dbfile.getTupleDesc();
        TransactionId tid = new TransactionId();
          
        // Get min, max for integer fields 
        Map<Integer, Integer> min = new HashMap<>(); // map field index to min
        Map<Integer, Integer> max = new HashMap<>(); // map field index to max
        
        try {
            DbFileIterator iter = dbfile.iterator(tid);
            iter.open();
            while(iter.hasNext()){
                this.totalTuples++;
                Tuple t = iter.next();
                
                // For int fields, calculate the running min/max
                for(int i = 0; i < td.numFields(); i++) {
                    if(td.getFieldType(i) == Type.INT_TYPE) {
                        IntField field = (IntField) t.getField(i);
                        int value = field.getValue();
                        
                        if(!min.containsKey(i)) {
                            min.put(i, value);
                            max.put(i, value);
                        }
                        else {
                            min.put(i, Math.min(min.get(i), value));
                            max.put(i, Math.max(max.get(i), value));
                        }
                    }
                }
            }
            iter.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
        
        // Generate histograms 
        for(int i = 0; i < td.numFields(); i++) {
            if(td.getFieldType(i) == Type.INT_TYPE) intHistograms.put(i, new IntHistogram(NUM_HIST_BINS, min.get(i), max.get(i)));
            else stringHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
        }
        
        // Populate histograms
        try {
            DbFileIterator iter = dbfile.iterator(tid);
            iter.open();
            while(iter.hasNext()) {
                Tuple t = iter.next();
                for(int i = 0; i < td.numFields(); i++) {
                    if(td.getFieldType(i) == Type.INT_TYPE) {
                        IntHistogram hist = intHistograms.get(i);
                        IntField field = (IntField) t.getField(i);
                        int value = field.getValue();
                        hist.addValue(value);
                    }
                    else {
                        StringHistogram hist = stringHistograms.get(i);
                        StringField field = (StringField) t.getField(i);
                        String value = field.getValue();
                        hist.addValue(value);
                    }
                }
            }
            iter.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
        
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return dbfile.numPages() * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (selectivityFactor * this.totalTuples());
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // Int Field
        if(intHistograms.containsKey(field)) {
            IntHistogram hist = intHistograms.get(field);
            int value = ((IntField) constant).getValue();
            return hist.estimateSelectivity(op, value); 
        }
        // String field
        else {
            StringHistogram hist = stringHistograms.get(field);
            String value = ((StringField) constant).getValue();
            return hist.estimateSelectivity(op, value); 
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return this.totalTuples;
    }

    /**
     * return the skew of given column in this table
     */
    public double getSkewForColumn(int i) {
        return intHistograms.get(i).getSkew();
    }
    
    /**
     * compute group counts in table for given column 
     * 
     * @param sampleSize 	the samplesize to use the cap on 
     * @param i 			index of the stratified column
     */
    public int calculateCapForColumn(int sampleSize, int i) {
    	IntHistogram intHist = intHistograms.get(i);
    	List<Integer> buckets = intHist.getFilledBuckets();
    	int numGroups = buckets.size();
    	return (int) Math.floor(sampleSize / numGroups);
    }
}
