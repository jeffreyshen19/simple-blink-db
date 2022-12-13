package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.optimizer.QueryColumnSet;
import simpledb.optimizer.SampleCreator;
import simpledb.optimizer.TableStats;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class SampleDBFile extends HeapFile{
    //private final File f;
    private final TupleDesc td;
    private final QueryColumnSet stratifiedColumns;
    private final List<Integer> sampleSizes;
    private final TransactionId tid;
    
    public SampleDBFile(File f, List<Integer> sampleSizes, QueryColumnSet stratifiedColumns, TupleDesc td) throws DbException, IOException, TransactionAbortedException {
        super(f, td);
        this.stratifiedColumns = stratifiedColumns;
        this.sampleSizes = sampleSizes;
        this.td = td;
        this.tid = new TransactionId();
    }
    
    /**
     * Populate the SampleDbFile based off origFile
     * @param origFile
     * @throws NoSuchElementException
     * @throws DbException
     * @throws TransactionAbortedException
     * @throws IOException
     */
    public void createUniformSamples(DbFile origFile) throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        // Sample maxSize integers from origFile using reservoir sampling (O(n))
        int maxSize = sampleSizes.get(sampleSizes.size() - 1); // k is the size of the *largest* sample
        List<Tuple> reservoir = Arrays.asList(new Tuple[maxSize]);
        
        DbFileIterator iterator = origFile.iterator(null);
        iterator.open();
        
        int i = 0;
        while(iterator.hasNext()) {
            Tuple tuple = iterator.next();
            
            if(i < maxSize) reservoir.set(i, tuple);
            else {
                int j = ThreadLocalRandom.current().nextInt(0, i + 1); // random integer in range [0, i]
                if(j < maxSize) reservoir.set(j, tuple);
            }      
            
            i++;
        }
        
        iterator.close();      
        
        // Write the tuples to disk
        Collections.shuffle(reservoir);
        
        for(i = 0; i < maxSize; i++) {
            Tuple tuple = reservoir.get(i);
            Database.getBufferPool().insertTuple(tid, this.getId(), tuple);
        }
    }

    public void createStratifiedSamples(DbFile origFile) throws DbException, IOException, TransactionAbortedException {
    	TableStats tableStats = new TableStats(origFile.getId(), SampleCreator.IO_COST); 
    	
    	// get indices of the stratified columns 	
        Set<Integer> colIndices = this.stratifiedColumns.getColumns();

        // Sample maxSize integers from origFile using reservoir sampling (O(n))
    	int maxSize = sampleSizes.get(sampleSizes.size() - 1); // maxSize is the size of the *largest* sample
        List<Tuple> reservoir = Arrays.asList(new Tuple[maxSize]);
        ConcurrentHashMap<String, Integer> columnValCount = new ConcurrentHashMap<>();
    	
    	DbFileIterator iterator = origFile.iterator(null);
        iterator.open();
        
        int i = 0;
        int currSampleI = 0;
        int currSize = sampleSizes.get(currSampleI);
        int currCap = Math.max(1, currSize / 6); // hardcoded with 6 at the moment 
        
        while(iterator.hasNext()) {
        	if (i == currSize) {
        		if (currSampleI < sampleSizes.size() - 1) {
        			currSampleI++;
            		currSize = sampleSizes.get(currSampleI);
            		currCap = Math.max(1, currSize / 3);
        		}
        		else {
        			currCap = Integer.MAX_VALUE;
        		}
        	}
        	
            Tuple tuple = iterator.next();
        	StringBuilder currStratColVal = new StringBuilder(); // current stratified columns' value
        	
            // check if we have reached the cap for given combination of stratifiedColumns for this sample
            for (Integer index : colIndices) {
            	currStratColVal.append(tuple.getField(index).toString());
            }
            
            if (columnValCount.containsKey(currStratColVal.toString()) && columnValCount.get(currStratColVal.toString()) == currCap) {
            	continue;
            }
            
            columnValCount.putIfAbsent(currStratColVal.toString(), 0);
            columnValCount.put(currStratColVal.toString(), columnValCount.get(currStratColVal.toString()) + 1);
            
            
            if(i < maxSize) reservoir.set(i, tuple);
            else if (sampleSizes.size() > 1){ // only randomly replace tuples in the larger sample to make sure that the smaller ones are distributed properly
                int j = ThreadLocalRandom.current().nextInt(0, i + 1); 
                if(sampleSizes.get(sampleSizes.size() - 2) <= j  && j < maxSize) reservoir.set(j, tuple);
            }         
            
            i++;
        }
             
        iterator.rewind();      
        
        // fills the sample in case we did not fill the samples due to cap
        for (i = 0; i < reservoir.size(); i++) {
        	if (reservoir.get(i) == null) {
        		reservoir.set(i, iterator.next());
        	}
        }
        
        iterator.close();
        
        // Write the tuples to disk
        Collections.shuffle(reservoir);
        
        for(i = 0; i < maxSize; i++) {
            Tuple tuple = reservoir.get(i);
            Database.getBufferPool().insertTuple(tid, this.getId(), tuple);
        }
    }

    public QueryColumnSet getStratifiedColumnSet() {
        return this.stratifiedColumns;
    }

    public List<Integer> getSampleSizes() {
        return this.sampleSizes;
    }

    public boolean isStratified() {
        return this.stratifiedColumns == null;
    }

    // this iterator should not get called- is only here to not throw errors
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new SampleIterator(this.getId(), tid, this.numPages(), this.sampleSizes.get(sampleSizes.size() - 1));
    }

    // this iterator is called for actually generating tuples
    // you must know that it is an existing 
    public DbFileIterator iterator(TransactionId tid, int cutoff) {
        return new SampleIterator(this.getId(), tid, this.numPages(), cutoff);
    }

}
