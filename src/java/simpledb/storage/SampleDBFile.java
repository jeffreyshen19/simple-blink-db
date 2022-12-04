package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.optimizer.QueryColumnSet;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SampleDBFile implements DbFile{
    private final File f;
    private final TupleDesc td;
    private final QueryColumnSet stratifiedColumns;
    private final List<Integer> sampleSizes;

    public SampleDBFile(File f, List<Integer> sampleSizes, QueryColumnSet stratifiedColumns, DbFile origFile) {
        this.f = f;
        this.stratifiedColumns = stratifiedColumns;
        this.sampleSizes = sampleSizes;

        this.td = origFile.getTupleDesc();

        if (this.stratifiedColumns == null) createUniformSamples(origFile);
        else createStratifiedSamples(origFile); // TODO: figure out the cap stuff @Yun
    }

    private void createUniformSamples(DbFile origFile) throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        // TODO: adapt to new data structure

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

    private void createStratifiedSamples(DbFile origFile, int cap) {
    	// TODO: adapt to new data structure
    	// get indices of the stratified columns
    	
    	List<Integer> colIndices = Arrays.asList(new Integer[this.stratifiedColumns.getNumCols()]);
    	Set<String> colNames = this.stratifiedColumns.getColumns();
    	for (int i = 0; i < this.td.numFields(); i++) {
    		if (colNames.contains(this.td.getFieldName(i))) {
    			colIndices.add(i);
    		}
    	}
    	
    	// get all columns from query column set, iterate through it, get the i using td
    	
    	int maxSize = sampleSizes.get(sampleSizes.size() - 1); // k is the size of the *largest* sample
        List<Tuple> reservoir = Arrays.asList(new Tuple[maxSize]);
        ConcurrentHashMap<String, Integer> columnValCount = new ConcurrentHashMap<>();
    	
    	DbFileIterator iterator = origFile.iterator(null);
        iterator.open();
        
        int i = 0;
        while(iterator.hasNext()) {
            Tuple tuple = iterator.next();
        	StringBuilder currStratColVal = new StringBuilder(); // current stratified columns' value
        	
            // check if we have reached the cap for given combination of stratifiedColumns for this sample
            for (Integer index : colIndices) {
            	currStratColVal.append(tuple.getField(index).toString());
            }
            
            if (columnValCount.containsKey(currStratColVal.toString()) && columnValCount.get(currStratColVal.toString()) == cap) {
            	continue;
            }
            
            columnValCount.putIfAbsent(currStratColVal.toString(), 0);
            columnValCount.put(currStratColVal.toString(), columnValCount.get(currStratColVal.toString()) + 1);
            
            
            if(i < maxSize) reservoir.set(i, tuple);
            else {
                int j = ThreadLocalRandom.current().nextInt(0, i + 1); // random integer in range [0, i]
                if(j < maxSize) reservoir.set(j, tuple);
            }      
            
            i++;
        }
        
        // Divide the k tuples into the correct sample files 
        Collections.shuffle(reservoir);
        int sampleI = 0; 
        int dbId = samples.get(sampleI).getId();
        int sampleSize = sampleSizes.get(sampleI);
        
        
        for(i = 0; i < maxSize; i++) {
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


    public File getFile() {
        return this.f;
    }

    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public QueryColumnSet getStratifiedColumnSet() {
        return this.stratifiedColumns;
    }

    public List<Integer> getSampleSizes() {
        return this.sampleSizes;
    }

    public int numPages() {
        return (int) (f.length() / BufferPool.getPageSize());
    }

    public boolean isStratified() {
        return this.stratifiedColumns == null;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(this.f, "rw");
        } catch (FileNotFoundException e2) {
            throw new IllegalArgumentException("");
        }
        
        byte[] data = new byte[BufferPool.getPageSize()];
        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        try {
            raf.seek(offset);
            raf.read(data, 0, data.length);
            raf.close();
        } catch (IOException e1) {
            throw new IllegalArgumentException("Page does not exist in file");
        }
    
        try {
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Page does not exist in file");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pgNo = page.getId().getPageNumber();

        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(this.f, "rw");
        } catch (FileNotFoundException e2) {
            throw new IllegalArgumentException("");
        }
        
        raf.seek(pgNo * BufferPool.getPageSize());
        raf.write(page.getPageData());
        raf.close();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
        int n = numPages();
        HeapPage page;
        
        for(int i = 0; i < n; i++) { // Go through all pages, find one with an empty slot 
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (page.getNumUnusedSlots() > 0) {
                page.insertTuple(t);
                return Arrays.asList(page);
            }
        }
        
        // Create a new page
        page = new HeapPage(new HeapPageId(this.getId(), n), HeapPage.createEmptyPageData());
        page.insertTuple(t);
        writePage(page);
        
        return Arrays.asList(page);
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage page;
        try {
            page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        }
        catch (IllegalArgumentException e) {
            throw new DbException("Tuple does not belong to this file");
        }
        
        page.deleteTuple(t);
        
        return Arrays.asList(page);
    }

    // this iterator should not get called- is only here to not throw errors
    public DbFileIterator iterator(TransactionId tid) {
        return new SampleIterator(this.getId(), tid, this.numPages(), this.sampleSizes.get(sampleSizes.size() - 1));
    }

    // this iterator is called for actually generating tuples
    // you must know that it is an existing 
    public DbFileIterator iterator(TransactionId tid, int cutoff) {
        return new SampleIterator(this.getId(), tid, this.numPages(), cutoff);
    }

}
