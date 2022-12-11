package simpledb.execution;

import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * If we are calculating aggregates using samples, the OpIterator tree 
 * should now go:
 *      SampleAggregate -> Aggregate -> ... 
 *               instead of 
 *      Aggregate -> ... 
 * 
 * SampleAggregate is performed on results from Aggregate which returns 
 * tuples of (groupBy, result) if the aggregate is performed over groups 
 * and (result) if there  is no  group by field
 * 
 */
public class SampleAggregate extends Operator {
    private OpIterator child; 
    private double sampleSize; 
    private int totalTups; 
    private Aggregator.Op op;
    private boolean grouping; // whether the child has group by field
    private int numTuples;
    private int totalTuples;
    
    public SampleAggregate(OpIterator child, int sampleSize, int totalTups, Aggregator.Op op) {
        this.child = child;
        this.sampleSize = sampleSize;
        this.totalTups = totalTups;
        this.op = op; 
        this.grouping = child.getTupleDesc().numFields() > 1;
        this.numTuples = child.numTuples();
        this.totalTuples = child.totalTuples();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        this.numTuples = child.numTuples();
        this.totalTuples = child.totalTuples();
    }

    @Override
    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        try {
            Tuple next = this.child.next();
            Tuple resultTuple = new Tuple(this.child.getTupleDesc());
            int aggVal, result;

            // get agg value performed on the samples
            if (grouping) {
                aggVal = ((IntField) next.getField(1)).getValue(); // if grouping -> (groupBy,  aggregate value)
                resultTuple.setField(0, next.getField(0)); // sets the groupBy field
            } else {
                aggVal = ((IntField) next.getField(0)).getValue();
            }

            // rescale agg value
            switch(op) {
                case SUM:
                    result = (int) (aggVal / sampleSize * totalTups);
                    break;
                case AVG:
                    result = aggVal;
                    break;
                case COUNT:
                    result = (int) (aggVal / sampleSize * totalTups);
                    break;
                default:
                    throw new DbException("Sample Aggregate only supports SUM, AVG, COUNT");
            }

            // assign new rescaled agg value
            if (grouping) {
                resultTuple.setField(1, new IntField(result));
            } else {
                resultTuple.setField(0, new IntField(result));
            }

            return resultTuple;
        }
        catch (NoSuchElementException e) {
            return null;
        }
    }
    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }
    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
        
    }
    @Override
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    } 

    @Override
    public int totalTuples() {
        return this.totalTuples;
    }

    @Override
    public int numTuples() {
        return this.numTuples;
    }
}
