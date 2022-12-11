package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private boolean deleted = false; // keep track of whether operator has been called
    private int numTuples;
    private int totalTuples;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.t = t;
        this.child = child;
        this.totalTuples = child.totalTuples();
        this.numTuples = child.numTuples();
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        this.numTuples = child.numTuples();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        deleted = false;
        this.numTuples = child.numTuples();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(deleted) return null;
        
        int numDeleted = 0;
        BufferPool bp = Database.getBufferPool();
        while(child.hasNext()) {
            try {
                bp.deleteTuple(t, child.next());
                numDeleted++;
            } catch (NoSuchElementException | DbException | IOException | TransactionAbortedException e) {
                e.printStackTrace();
            }
        }
        
        deleted = true;
        
        Tuple result = new Tuple(getTupleDesc());
        result.setField(0, new IntField(numDeleted));
        this.numTuples--;
        return result;
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
    public int totalTuples() {
        return this.totalTuples;
    }
    @Override
    public int numTuples() {
        return this.numTuples;
    }
}
