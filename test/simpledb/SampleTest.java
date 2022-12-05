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
import simpledb.common.Utility;
import simpledb.optimizer.QueryColumnSet;
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
        hf = SystemTestUtil.createRandomHeapFile(2, 10000, null, null);
        td = Utility.getTupleDesc(2);
    }
    
    @Test
    public void testUniformSample() throws Exception {
        // Create sample table and add it to catalog
        List<Integer> sampleSizes = Arrays.asList(10, 100, 1000);
        File f = File.createTempFile("sample-table", "dat");
        f.deleteOnExit();
        SampleDBFile sf = new SampleDBFile(f, sampleSizes, null, this.hf);
        Database.getCatalog().addTable(sf, "sample-table", "", true);
        
        // Populate sample table
        sf.createUniformSamples();
        
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
