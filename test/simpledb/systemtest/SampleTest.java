package simpledb.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Aggregate;
import simpledb.execution.Aggregator;
import simpledb.execution.Filter;
import simpledb.execution.OpIterator;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.optimizer.SampleSelector;
import simpledb.storage.BufferPool;
import simpledb.storage.HeapFile;
import simpledb.storage.HeapFileEncoder;
import simpledb.storage.IntField;
import simpledb.storage.SampleDBFile;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

public class SampleTest {

    private HeapFile hf;
    private SampleDBFile sf;
    private TransactionId tid;
    private List<Integer> sampleSizes;
    private List<OpIterator> queries;
    private TupleDesc td;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"id", "quantity", "year"};
        this.td = new TupleDesc(types, names);
        
        System.out.println("starting evaluation");
        
        hf = new HeapFile(new File("test_dataset_50M.dat"), td);
        Database.getCatalog().addTable(hf, "t1");
        
        System.out.println("read original file");
    
        // Create sample table and add it to catalog
        sampleSizes = Arrays.asList(10000, 50000, 100000, 1000000);
        File f = new File("sample.dat");
        sf = new SampleDBFile(f, sampleSizes, null, this.td);
        Database.getCatalog().addTable(sf, "sample-table", "", true);
        
        // Populate sample table (if it hasn't already been populated)
        if(!f.exists()) {
            sf.createUniformSamples(this.hf);
            Database.getBufferPool().flushAllPages();
        }
        
        System.out.println("Finished generating samples");
        
        // Add queries to query workload 
        this.queries = getQueryWorkload();
    }
    
    private List<OpIterator> getQueryWorkload(){
        List<OpIterator> queries = new ArrayList<>();
        OpIterator seqscan = new SeqScan(null, hf.getId(), "");
        
        // SELECT COUNT/AVG/SUM(quantity) FROM table;
        queries.add(new Aggregate(seqscan, 1, -1, Aggregator.Op.COUNT)); 
        queries.add(new Aggregate(seqscan, 1, -1, Aggregator.Op.AVG)); 
        queries.add(new Aggregate(seqscan, 1, -1, Aggregator.Op.SUM)); 
        
        // SELECT COUNT/AVG/SUM(quantity) FROM table WHERE quantity < 50
        Predicate p = new Predicate(1, Predicate.Op.LESS_THAN, new IntField(50));
        OpIterator filter = new Filter(p, seqscan);
        queries.add(new Aggregate(filter, 1, -1, Aggregator.Op.COUNT)); 
        queries.add(new Aggregate(filter, 1, -1, Aggregator.Op.AVG)); 
        queries.add(new Aggregate(filter, 1, -1, Aggregator.Op.SUM)); 
        
        // SELECT COUNT/AVG/SUM(quantity) FROM table WHERE YEAR >= 2013 AND QUANTITY > 10
        Predicate p1 = new Predicate(1, Predicate.Op.GREATER_THAN, new IntField(10));
        Predicate p2 = new Predicate(2, Predicate.Op.GREATER_THAN_OR_EQ, new IntField(2013));
        OpIterator filter1 = new Filter(p2, seqscan);
        OpIterator filter2 = new Filter(p1, filter1);
        queries.add(new Aggregate(filter2, 1, -1, Aggregator.Op.COUNT));
        queries.add(new Aggregate(filter2, 1, -1, Aggregator.Op.AVG));
        queries.add(new Aggregate(filter2, 1, -1, Aggregator.Op.SUM));
        
        // SELECT COUNT/AVG/SUM(quantity) FROM table WHERE year = 2010
        p = new Predicate(2, Predicate.Op.EQUALS, new IntField(2010));
        filter = new Filter(p, seqscan);
        queries.add(new Aggregate(filter, 1, -1, Aggregator.Op.COUNT)); 
        queries.add(new Aggregate(filter, 1, -1, Aggregator.Op.AVG)); 
        queries.add(new Aggregate(filter, 1, -1, Aggregator.Op.SUM)); 
        
        // SELECT COUNT/AVG/SUM(quantity) FROM table GROUP BY year
        queries.add(new Aggregate(seqscan, 1, 2, Aggregator.Op.COUNT)); 
        queries.add(new Aggregate(seqscan, 1, 2, Aggregator.Op.AVG)); 
        queries.add(new Aggregate(seqscan, 1, 2, Aggregator.Op.SUM));
        
        // SELECT COUNT/AVG/SUM(quantity) FROM table WHERE quantity < 20 GROUP BY year
        p = new Predicate(1, Predicate.Op.LESS_THAN, new IntField(20));
        filter = new Filter(p, seqscan);
        queries.add(new Aggregate(filter, 1, 2, Aggregator.Op.COUNT)); 
        queries.add(new Aggregate(filter, 1, 2, Aggregator.Op.AVG)); 
        queries.add(new Aggregate(filter, 1, 2, Aggregator.Op.SUM)); 
        
        return queries;
    }

    /**
     * Test Error
    */
    @Test 
    public void testErrorSelection() throws Exception{
        TransactionId tid = new TransactionId();
        OpIterator seqscan = new SeqScan(tid, hf.getId(), "");
        // SELECT COUNT/AVG/SUM(quantity) FROM table WHERE quantity < 50
        Predicate p = new Predicate(1, Predicate.Op.LESS_THAN, new IntField(50));
        OpIterator filter = new Filter(p, seqscan);
        OpIterator query = new Aggregate(filter, 1, -1, Aggregator.Op.COUNT);
        
        int n = SampleSelector.selectSampleSizeError(sf.getId(), sampleSizes.get(0), 5000000,  query, 0.95);
        System.out.println("selected n:" + n);
        double actualError = SampleSelector.calculateError(sf.getId(), n, 5000000, query);
        
    }
    
    /**
     * Test Latency
    */
    @Test 
    public void testLatencySelection() throws Exception{
        TransactionId tid = new TransactionId();
        OpIterator query = new SeqScan(tid, hf.getId(), "");
        int targetTime = 50; // ms
        int n = SampleSelector.selectSampleSizeLatency(sf.getId(), sampleSizes, query, targetTime); 
        // N = # of rows to read to have a targetTime runtime 
        
        // Try running a new query with that n, see if the time is roughly correct.
        Database.getBufferPool().clearBufferPool();
        int actualTime = SampleSelector.timeQueryOnSample(sf.getId(), query, n);
        assertEquals(actualTime < targetTime, true);
    }
    
    /*
     * Generate graph data to plot requested response time v actual response time
     */
    @Test
    public void evaluateLatencySelection() throws Exception {
        
        FileWriter outputfile = new FileWriter("latency-evaluation.csv");
        
        String result = "";
        
        // every 5 ms up to 75ms
        for(int targetTime = 5; targetTime <= 75; targetTime += 5) {
            result += targetTime;
            
            for(int i = 0; i < queries.size(); i++) {
                OpIterator query = queries.get(i);
                int n = SampleSelector.selectSampleSizeLatency(sf.getId(), sampleSizes, query, targetTime);
                Database.getBufferPool().clearBufferPool();
                int actualTime = SampleSelector.timeQueryOnSample(sf.getId(), query, n);
                result += "," + actualTime;
                
            }

            result += "\n";
            
        }
        
        outputfile.write(result);
        outputfile.close();
        
    }
     
    
}
