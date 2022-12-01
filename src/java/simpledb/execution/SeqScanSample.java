package simpledb.execution;

import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.SampleFamily;
import simpledb.storage.SampleIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleDesc.TDItem;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * SeqScanSample is an implementation of a sequential scan that 
 * reads tuples from a SampleFamily
 */
public class SeqScanSample implements OpIterator {
    
    private SampleFamily sampleFamily; 
    private SampleIterator iterator;
    private boolean opened = false;
    
    /**
     * Creates a sequential scan over the specified sample family of size n
     * @param sampleFamily
     * @param n
     */
    public SeqScanSample(SampleFamily sampleFamily, int n) {
        this.sampleFamily = sampleFamily;
        this.iterator = sampleFamily.iterator(n, null);
    }

    public void open() throws DbException, TransactionAbortedException {
        this.opened = true;
        this.iterator.open();
    }
    
    /**
     * Returns the TupleDesc of the sampleFamily
     */
    public TupleDesc getTupleDesc() {        
        return this.sampleFamily.getTupleDesc();
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if(!this.opened) throw new IllegalStateException("SeqScanSample not opened");
        return this.iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Tuple nextTuple = iterator.next();
        nextTuple.resetTupleDesc(this.getTupleDesc());
        return nextTuple;
    }

    public void close() {
        this.opened = false;
        this.iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.iterator.rewind();
    }
}
