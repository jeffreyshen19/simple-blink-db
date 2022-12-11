package simpledb.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.OpIterator;
import simpledb.execution.SeqScan;
import simpledb.optimizer.SampleSelector;
import simpledb.storage.BufferPool;
import simpledb.storage.HeapFile;
import simpledb.storage.HeapFileEncoder;
import simpledb.storage.SampleDBFile;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

public class SampleTest {

    private HeapFile hf;
    private SampleDBFile sf;
    private TransactionId tid;
    private List<Integer> sampleSizes;
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
        OpIterator query = new SeqScan(null, hf.getId(), "");
        
        // every 5 ms up to 75ms
        for(int targetTime = 5; targetTime <= 75; targetTime += 5) {
            System.out.print(targetTime);
            
            for(int i = 0; i < 10; i++) {
                int n = SampleSelector.selectSampleSizeLatency(sf.getId(), sampleSizes, query, targetTime);
                Database.getBufferPool().clearBufferPool();
                int actualTime = SampleSelector.timeQueryOnSample(sf.getId(), query, n);
                System.out.print("," + actualTime);
                
            }

            System.out.println();
            
        }
        
    }
     
    
}
