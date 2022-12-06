package simpledb;

import static org.junit.Assert.assertEquals;
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

import simpledb.common.Type;
import simpledb.common.Database;
import simpledb.common.Utility;
import simpledb.execution.OpIterator;
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
import simpledb.systemtest.SystemTestUtil;
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
        
        hf = new HeapFile(new File("test_uniform_dataset_5000000.dat"), td);
        Database.getCatalog().addTable(hf, "t1");
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
        Database.getBufferPool().flushAllPages();
        
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
