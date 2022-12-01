package simpledb.storage;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * SampleWrapper is a wrapper for multiple DbFiles to store different sized samples in one object
 * The samples inside are part of the same sample family
 * Samples are stored as separate DBFiles, 
 * similarly to BlinkDB's description of samples building on top of each other
 */
public class SampleFamily {
    final List<Integer> sampleSizes;
    final List<File> files;
    final TupleDesc stratifiedColumns; // TODO: this should be changed to a QueryColumnSet
    final List<DbFile> samples;
    final TupleDesc td;

    /**
     * Create SampleWrapper for a table
     * 
     * @param sampleSizes - list of sample sizes - must be in increasing order
     * @param stratifiedColumns - columns to stratify sample on
     *                       if stratifiedColumns is null, then this is a uniform sample
     * @param origFile - the file from which the samples are being created from 
     * @throws TransactionAbortedException 
     * @throws DbException 
     * @throws NoSuchElementException 
     * @throws IOException 
     */
    public SampleFamily(List<Integer> sampleSizes, List<File> files, TupleDesc stratifiedColumns, DbFile origFile) throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        this.sampleSizes = sampleSizes;
        this.files = files;
        this.stratifiedColumns = stratifiedColumns;
        
        String origName = Database.getCatalog().getTableName(origFile.getId());
        this.td = origFile.getTupleDesc();
        
        // Generate DbFiles for each sample family 
        this.samples = new ArrayList<>();
        for(int i = 0; i < sampleSizes.size(); i++) {
            DbFile db = new HeapFile(files.get(i), origFile.getTupleDesc());
            Database.getCatalog().addTable(db, origName + "-sample-" + i); //TODO: deal with catalog when regenerating sample
            this.samples.add(db);
        }
        
        // Populate the samples
        if (this.stratifiedColumns == null) createUniformSamples(origFile);
        else createStratifiedSamples(origFile);
    }
    
    public List<DbFile> getSamples() {
        return this.samples;
    }
    
    public TupleDesc getTupleDesc() {
        return this.td;
    }
    
    private void createUniformSamples(DbFile origFile) throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        
        // Sample k integers from origFile using reservoir sampling (O(n))
        int k = sampleSizes.get(sampleSizes.size() - 1); // k is the size of the *largest* sample
        List<Tuple> reservoir = Arrays.asList(new Tuple[k]);
        
        DbFileIterator iterator = origFile.iterator(null);
        iterator.open();
        
        int i = 0;
        while(iterator.hasNext()) {
            Tuple tuple = iterator.next();
            
            if(i < k) reservoir.set(i, tuple);
            else {
                int j = ThreadLocalRandom.current().nextInt(0, i + 1); // random integer in range [0, i]
                if(j < k) reservoir.set(j, tuple);
            }      
            
            i++;
        }
        
        // Divide the k tuples into the correct sample files 
        Collections.shuffle(reservoir);
        int sampleI = 0; 
        int dbId = samples.get(sampleI).getId();
        int sampleSize = sampleSizes.get(sampleI);
        
        
        for(i = 0; i < k; i++) {
            // If sampleSizes are [a, b, c, ...],
            // add tuples with i < a to samples[0], tuples with i < b to samples[1], etc
            if(i == sampleSize) {
                sampleI++;
                dbId = samples.get(sampleI).getId();
                sampleSize = sampleSizes.get(sampleI);
            }
            
            Tuple tuple = reservoir.get(i);
            Database.getBufferPool().insertTuple(null, dbId, tuple);
            
        }
             
        iterator.close();         
    }

    private void createStratifiedSamples(DbFile origFile) {
        return;   
    }

    public boolean isStratified() {
        return this.stratifiedColumns == null;
    }

    /**
     * Create an iterator for this sample
     * @param sampleSize - the max size sample to pull the query from - must be less than number of samples given by initializer
     * @param tid - transaction id
     * @return - an iterator that will return sampleSize number of tuples 
     */
    public SampleIterator iterator(int maxSample, TransactionId tid) {
        return new SampleIterator(samples, maxSample, tid);
    }
    
}
