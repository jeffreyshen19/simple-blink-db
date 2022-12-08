package simpledb.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.OpIterator;
import simpledb.execution.SeqScan;
import simpledb.optimizer.SampleSelector;
import simpledb.storage.HeapFile;
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
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"id", "quantity"};
        this.td = new TupleDesc(types, names);
        
        hf = new HeapFile(new File("test_uniform_dataset_5000000.dat"), td);
        Database.getCatalog().addTable(hf, "t1");
        
        // Create sample table and add it to catalog
        sampleSizes = Arrays.asList(10000, 50000, 200000);
        File f = File.createTempFile("sample-table", "dat");
        f.deleteOnExit();
        sf = new SampleDBFile(f, sampleSizes, null, this.td);
        Database.getCatalog().addTable(sf, "sample-table", "", true);
        
        // Populate sample table
        sf.createUniformSamples(this.hf);
        
        System.out.println("Finished generating samples");
    }
    
    /**
     * Test Latency
    */
    @Test 
    public void testLatencySelection() throws Exception{
        OpIterator query = new SeqScan(null, hf.getId(), "");
        int targetTime = 10; // ms
        int n = SampleSelector.selectSampleSizeLatency(sf.getId(), sampleSizes, query, targetTime); 
        // N = # of rows to read to have a targetTime runtime 
        
        // Try running a new query with that n, see if the time is roughly correct.
        int actualTime = SampleSelector.timeQueryOnSample(sf.getId(), query, n);
        assertEquals(actualTime, targetTime, 2);
    }
     
    
}
