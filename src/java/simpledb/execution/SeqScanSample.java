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
    }

    private void setSampleFile() {
        this.sampleFile = Database.getCatalog().getSampleDBFile(this.sampleFileTableId);
        this.iterator = this.sampleFile.iterator(tid, nTups);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.opened = true;
        this.iterator.open();
        
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(!this.opened) throw new IllegalStateException("SeqScan not opened");
        return this.iterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return iterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.iterator.rewind();
        
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

    
}
