package simpledb.execution;

import java.io.IOException;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t; 
    private OpIterator child;
    private int tableId;
    private boolean inserted = false; // keep track of whether operator has been called

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        if(!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc())) throw new DbException("TupleDesc of child differs from table");
        this.t = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        inserted = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(inserted) return null;
        
        int numInserted = 0;
        BufferPool bp = Database.getBufferPool();
        while(child.hasNext()) {
            try {
                bp.insertTuple(t, tableId, child.next());
                numInserted++;
            } catch (NoSuchElementException | DbException | IOException | TransactionAbortedException e) {
                e.printStackTrace();
            }
        }
        
        inserted = true;
        
        Tuple result = new Tuple(getTupleDesc());
        result.setField(0, new IntField(numInserted));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
