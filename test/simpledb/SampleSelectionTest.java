package simpledb;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.common.Utility;
import simpledb.execution.Aggregate;
import simpledb.execution.Aggregator;
import simpledb.execution.Filter;
import simpledb.execution.OpIterator;
import simpledb.execution.Operator;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.execution.SeqScanSample;
import simpledb.execution.Predicate.Op;
import simpledb.optimizer.QueryColumnSet;
import simpledb.optimizer.SampleSelector;
import simpledb.storage.BufferPool;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Field;
import simpledb.storage.HeapFile;
import simpledb.storage.HeapFileEncoder;
import simpledb.storage.SampleDBFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.systemtest.SystemTestUtil;
import simpledb.transaction.TransactionId;

public class SampleSelectionTest {
    
    private HeapFile hf;
    private TransactionId tid;
    private TupleDesc td;
    private SampleDBFile sf;
    private List<Integer> sampleSizes;
    
    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"id", "quantity"};
        td = new TupleDesc(types, names);
        sampleSizes = Arrays.asList(10000, 50000, 100000);
        sf = new SampleDBFile(new File("test_uniform_sample.dat"), sampleSizes, null, td);
        Database.getCatalog().addTable(sf, "sample-table", "", true);
    }
    
//    /**
//     * Test Latency
//     */
//    @Test 
//    public void testLatencySelection() throws Exception{
//        OpIterator query = new SeqScan(null, sf.getId(), "");
//        int n = SampleSelector.selectSampleSizeLatency(sf.getId(), sampleSizes, query, 500);
//        System.out.println(n);
//    }
    
    /**
     * Test that modifyOperatorSampleFamily replaces operator trees with a SeqScanOperator
     */
    
    // Replaces a SeqScan with SeqScanSample
    @Test
    public void testModifyOperatorSampleFamilySeqScan() throws Exception {
        final int N_TUPS = 100;
        OpIterator operator = new SeqScan(null, sf.getId(), "");
        OpIterator newOperator = SampleSelector.modifyOperatorSampleFamily(sf.getId(), operator, N_TUPS);
        
        assertEquals(newOperator instanceof SeqScanSample, true);
        SeqScanSample seqscanOperator = (SeqScanSample) newOperator;
        assertEquals(seqscanOperator.getNTups(), N_TUPS);
        assertEquals(seqscanOperator.getSampleFileTableId(), sf.getId());
    }
    
    // Replaces a Filter -> SeqScan with Filter -> SeqScanSample
    @Test
    public void testModifyOperatorSampleFamilyFilter() throws Exception {
        final int N_TUPS = 100;
        SeqScan scan = new SeqScan(null, sf.getId(), "");
        Predicate pred = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(0));
        OpIterator operator = new Filter(pred, scan);
        OpIterator newOperator = SampleSelector.modifyOperatorSampleFamily(sf.getId(), operator, N_TUPS);
        
        // First operator is unchanged
        assertEquals(newOperator instanceof Operator, true);
        assertEquals(newOperator instanceof Filter, true);
        Filter filterOperator = (Filter) newOperator;
        OpIterator[] children = filterOperator.getChildren();
        assertEquals(children.length, 1);
        
        // Child has been changed
        assertEquals(children[0] instanceof SeqScanSample, true);   
        SeqScanSample seqscanOperator = (SeqScanSample) children[0];
        assertEquals(seqscanOperator.getNTups(), N_TUPS);
        assertEquals(seqscanOperator.getSampleFileTableId(), sf.getId());
    }
    
    // Replaces an Aggregate -> Filter -> SeqScan with Aggregate -> Filter -> SeqScanSample
    @Test
    public void testModifyOperatorSampleFamilyAggregate() throws Exception {
        final int N_TUPS = 100;
        SeqScan scan = new SeqScan(null, sf.getId(), "");
        Predicate pred = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(0));
        Filter filter = new Filter(pred, scan);
        OpIterator operator = new Aggregate(filter, 0, 0, Aggregator.Op.MIN);
        OpIterator newOperator = SampleSelector.modifyOperatorSampleFamily(sf.getId(), operator, N_TUPS);
        
        // Aggregate is unchanged
        assertEquals(newOperator instanceof Operator, true);
        assertEquals(newOperator instanceof Aggregate, true);
        Aggregate aggregateOperator = (Aggregate) newOperator;
        OpIterator[] children = aggregateOperator.getChildren();
        assertEquals(children.length, 1);
        
        // Filter is unchanged
        assertEquals(children[0] instanceof Operator, true);
        assertEquals(children[0] instanceof Filter, true);
        Filter filterOperator = (Filter) children[0];
        children = filterOperator.getChildren();
        assertEquals(children.length, 1);
        
        // Child has been changed
        assertEquals(children[0] instanceof SeqScanSample, true);   
        SeqScanSample seqscanOperator = (SeqScanSample) children[0];
        assertEquals(seqscanOperator.getNTups(), N_TUPS);
        assertEquals(seqscanOperator.getSampleFileTableId(), sf.getId());
    }
    
    /**
     * Test timeQueryOnSample and runOperator 
     */
    @Test
    public void testTimeQueryOnSample() throws Exception {
        final int N_TUPS = 1000;
        OpIterator operator = new SeqScan(null, sf.getId(), "");
        OpIterator newOperator = SampleSelector.modifyOperatorSampleFamily(sf.getId(), operator, N_TUPS);
        int ms = SampleSelector.timeQueryOnSample(sf.getId(), operator, N_TUPS);
    }
    
}