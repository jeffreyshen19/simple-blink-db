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
    
    /**
     * Creates a sequential scan over the specified sample family of size n
     * @param sampleFamily the tableid of the sample family
     * @param n
     */
    public SeqScanSample(int sampleFamily, int n) {
    }

}
