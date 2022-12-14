package simpledb.execution;

import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.SampleDBFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * SeqScanSample is an implementation of a sequential scan that 
 * reads tuples from a SampleFamily
 */
public class SeqScanSample implements OpIterator {
    private final TransactionId tid;
    private final int sampleFileTableId;    
    private final int nTups;

    private SampleDBFile sampleFile;
    private DbFileIterator iterator;

    private boolean opened = false;
    private int numTuples;
    
    /**
     * Creates a sequential scan over the specified sample family of size n
     * @param sampleFileTableId the tableid of the sample family
     * @param nTups the size of the sampmle family
     */
    public SeqScanSample(TransactionId tid, int sampleFileTableId, int nTups) {
        this.tid = tid;
        this.sampleFileTableId = sampleFileTableId;
        this.nTups = nTups;
        this.setSampleFile();
        this.numTuples = 0;
    }

    private void setSampleFile() {
        this.sampleFile = Database.getCatalog().getSampleDBFile(this.sampleFileTableId);
        this.iterator = this.sampleFile.iterator(tid, nTups);
    }
    
    public int getNTups() {
        return this.nTups;
    }
    
    public int getSampleFileTableId() {
        return this.sampleFileTableId;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.opened = true;
        this.iterator.open();
        this.numTuples = 0;
        
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(!this.opened) throw new IllegalStateException("SeqScan not opened");
        return this.iterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        numTuples++;
        return iterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.iterator.rewind();
        this.numTuples = 0;
        
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.sampleFile.getTupleDesc();
    }

    @Override
    public void close() {
        this.opened = false;
        this.iterator.close();
        
    }

    public int totalTuples() {
        return this.numTuples;
    }
    
    public int numTuples() {
        return this.numTuples;
    }
}
