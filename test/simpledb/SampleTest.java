package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import simpledb.common.Type;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.JUnit4TestAdapter;
import simpledb.common.Type;
import simpledb.common.Database;
import simpledb.common.Utility;
import simpledb.execution.OpIterator;
import simpledb.execution.SeqScanSample;
import simpledb.optimizer.QueryColumnSet;
import simpledb.optimizer.SampleSelector;
import simpledb.storage.BufferPool;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.HeapFile;
import simpledb.storage.HeapFileEncoder;
import simpledb.storage.SampleDBFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;
import simpledb.transaction.TransactionId;

public class SampleTest extends SimpleDbTestBase{
    
    private HeapFile hf;
    private TransactionId tid;
    private TupleDesc td;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
//        hf = SystemTestUtil.createRandomHeapFile(2, 10000, null, null);
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"id", "quantity"};
        this.td = new TupleDesc(types, names);
        
        hf = new HeapFile(new File("test_uniform_dataset_5000000.dat"), td);
        Database.getCatalog().addTable(hf, "t1");
        
    }

    /**
     * Test SeqScanSample after generating a uniform sample 
     */
    @Test
    public void testSeqScanSample() throws Exception{
        TransactionId tid = new TransactionId();
        
        // Create sample table and add it to catalog
        List<Integer> sampleSizes = Arrays.asList(10000, 50000, 100000);
        File f = File.createTempFile("sample-table", "dat");
        f.deleteOnExit();
        SampleDBFile sf = new SampleDBFile(f, sampleSizes, null, this.td);
        Database.getCatalog().addTable(sf, "sample-table", "", true);
        
        // Populate sample table
        sf.createUniformSamples(this.hf);

        int tableId = Database.getCatalog().getTableId("sample-table");
        SeqScanSample scan = new SeqScanSample(tid, tableId, 50);
        scan.open();
        for (int i = 0; i < 50; ++i) {
            assertTrue(scan.hasNext());
            scan.next();
        }
        scan.rewind();
        for (int i = 0; i < 50; ++i) {
            assertTrue(scan.hasNext());
            scan.next();
        }
        assertFalse("There should be no more samples", !scan.hasNext());
        scan.close();
        Database.getBufferPool().transactionComplete(tid);
    }
    
    /**
     * Test generating a uniform sample 
     * @throws Exception
     */
    @Test
    public void testUniformSample() throws Exception {
        // Create sample table and add it to catalog
        List<Integer> sampleSizes = Arrays.asList(10000, 50000, 100000);
        File f = File.createTempFile("sample-table", "dat");
        f.deleteOnExit();
        SampleDBFile sf = new SampleDBFile(f, sampleSizes, null, this.td);
        Database.getCatalog().addTable(sf, "sample-table", "", true);
        
        // Populate sample table
        sf.createUniformSamples(this.hf);

        // Iterate through sample to ensure it was generated correctly
        Set<Tuple> sampledTuples = new HashSet<Tuple>();
        DbFileIterator iterator = sf.iterator(null);
        iterator.open();
        int counter = 0;
        while(iterator.hasNext()) {
            counter++;
            Tuple tuple = iterator.next();
            sampledTuples.add(tuple);
        }
        iterator.close();
        int expected = sampleSizes.get(sampleSizes.size() - 1);
        assertEquals(expected, counter); // Samples are the size we expect them to be
        
        
        assertEquals((int) sampleSizes.get(sampleSizes.size() - 1), sampledTuples.size()); // There are no repeated tuples
    }
    
}
