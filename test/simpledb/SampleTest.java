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
import simpledb.storage.BufferPool;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.HeapFile;
import simpledb.storage.HeapFileEncoder;
import simpledb.storage.SampleFamily;
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
        List<Integer> sampleSizes = Arrays.asList(10, 100, 1000);
        List<File> files = new ArrayList<>();
        for(int i = 0; i < sampleSizes.size(); i++) {
            File f = File.createTempFile("sample-table-" + i, ".dat");
            f.deleteOnExit();
            files.add(f);
        }
        
        SampleFamily sf = new SampleFamily(sampleSizes, files, null, this.hf);
        Set<Tuple> sampledTuples = new HashSet<Tuple>();
        
        for(int i = 0; i < sf.getSamples().size(); i++) {
            DbFile sample = sf.getSamples().get(i);
            DbFileIterator iterator = sample.iterator(null);
            iterator.open();
            int counter = 0;
            while(iterator.hasNext()) {
                counter++;
                Tuple tuple = iterator.next();
                sampledTuples.add(tuple);
            }
            iterator.close();
            int expected = sampleSizes.get(i) - (i == 0 ? 0 : sampleSizes.get(i - 1));
            assertEquals(expected, counter); // Samples are the size we expect them to be
        }
        
        assertEquals((int) sampleSizes.get(sampleSizes.size() - 1), sampledTuples.size()); // There are no repeated tuples
    }
}
