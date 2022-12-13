package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import simpledb.common.Type;

import org.junit.Before;
import org.junit.Test;

import simpledb.common.Database;
import simpledb.execution.SeqScanSample;
import simpledb.optimizer.QueryColumnSet;
import simpledb.storage.BufferPool;
import simpledb.storage.DbFileIterator;
import simpledb.storage.HeapFile;
import simpledb.storage.HeapFileEncoder;
import simpledb.storage.IntField;
import simpledb.storage.SampleDBFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

public class SampleTest {
    
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
        
        hf = new HeapFile(new File("test_dataset_5M.dat"), td);
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
        assertFalse("There should be no more samples", scan.hasNext());
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

    /**
     * Test generating a uniform sample 
     * @throws Exception
     */
    @Test
    public void testStratifiedSampleSingleColumn() throws Exception {
        // Create a small Heapfile with skewed data [1,1,1,1,1,2,3]
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"id", "quantity"};
        
        TupleDesc tupleDesc = new TupleDesc(types, names);
        File sourceTxtFile = new File("test_small_skewed_dataset.txt");
        File targetDatFile = new File("test_small_skewed_dataset.dat");
        HeapFileEncoder.convert(sourceTxtFile, targetDatFile,
                            BufferPool.getPageSize(), 2, types, ',');
        HeapFile test_hf = new HeapFile(new File("test_small_skewed_dataset.dat"), td);
        Database.getCatalog().addTable(test_hf, "t2");

        // Create sample table and add it to catalog we want a max sample size of 5
        List<Integer> sampleSizes = Arrays.asList(5);
        File f = File.createTempFile("sample-table", "dat");
        f.deleteOnExit();

        QueryColumnSet qcs = new QueryColumnSet(1);
        SampleDBFile sf = new SampleDBFile(f, sampleSizes, qcs, tupleDesc);
        Database.getCatalog().addTable(sf, "sample-table", "", true);

        // Populate sample table
        sf.createStratifiedSamples(test_hf);

        // Iterate through sample to ensure it was generated correctly
        DbFileIterator iterator = sf.iterator(null);
        iterator.open();

        Map<Integer, Integer> quantityCount = new HashMap<>();
        int counter = 0;
        while(iterator.hasNext()) {
            Tuple tuple = iterator.next();
            int quantity = ((IntField) tuple.getField(1)).getValue();
            quantityCount.putIfAbsent(quantity, 0);
            quantityCount.put(quantity, quantityCount.get(quantity) + 1);
            counter++; 
        }
        iterator.close();
        int expected = sampleSizes.get(sampleSizes.size() - 1);
        assertEquals(expected, counter); // Samples are the size we expect them to be
        
        
        assertEquals(3, (int) quantityCount.get(1));
        assertEquals(1, (int) quantityCount.get(2));
        assertEquals(1, (int) quantityCount.get(3));

    }
    
}
