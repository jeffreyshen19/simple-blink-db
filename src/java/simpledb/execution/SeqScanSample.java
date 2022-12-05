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
    
    
}
