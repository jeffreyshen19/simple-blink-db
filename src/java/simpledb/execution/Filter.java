package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate p;
    private OpIterator child;
    private int totalTuples;
    private int numTuples;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.p = p;
        this.child = child;
        this.totalTuples = child.totalTuples();
        this.numTuples = 0;
    }

    public Predicate getPredicate() {
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this.child.open();
        this.numTuples = 0;
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        this.numTuples = 0;
        this.totalTuples = child.totalTuples();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {     
        Tuple next;
        
        do {
            if(!child.hasNext()) return null;
            next = child.next();
        }
        while(!this.p.filter(next));
        this.numTuples++;
        return next;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
    @Override
    public int totalTuples() {
        return this.child.totalTuples();
    }
    @Override
    public int numTuples() {
        return this.numTuples;
    }
}