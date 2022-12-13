package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p; 
    private OpIterator child1;
    private OpIterator child2; 
    private Tuple t1; // Tuple from child1 we are on.
    private int totalTuples;
    private int numTuples;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.totalTuples = child1.totalTuples() + child2.totalTuples();
        this.numTuples = child1.numTuples() + child2.numTuples();
    }

    public JoinPredicate getJoinPredicate() {
        return this.p;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField1Name() {
        return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField2Name() {
        return this.child2.getTupleDesc().getFieldName(this.p.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *         implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(this.child1.getTupleDesc(), this.child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this.child1.open();
        this.child2.open();
        this.totalTuples = child1.totalTuples() + child2.totalTuples();
        this.numTuples = child1.numTuples() + child2.numTuples();
    }

    public void close() {
        super.close();
        this.child1.close();
        this.child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.t1 = null;
        this.child1.rewind();
        this.child2.rewind();
        this.totalTuples = child1.totalTuples() + child2.totalTuples();
        this.numTuples = child1.numTuples() + child2.numTuples();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // If the outer loop hasn't been initialized
        if(t1 == null) {
            if(this.child1.hasNext()) t1 = this.child1.next();
            else return null;
        }
        Tuple t2;
        
        // Find the next t1, t2 satisfying predicate
        do {
            if(!this.child2.hasNext()) {
                if(this.child1.hasNext()) { // End of inner loop, continue outer loop
                    t1 = this.child1.next();
                    this.child2.rewind();
                }
                else return null; // End of both inner and outer loop
            }
            
            t2 = this.child2.next();
        }
        while(!this.p.filter(t1, t2));
        
        // Return joined tuple
        Tuple result = new Tuple(this.getTupleDesc());
        
        for(int i = 0; i < t1.getTupleDesc().numFields(); i++) {
            result.setField(i, t1.getField(i));
        }
        for(int i = 0; i < t2.getTupleDesc().numFields(); i++) {
            result.setField(i + t1.getTupleDesc().numFields(), t2.getField(i));
        }
        
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {this.child1, this.child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }

    @Override
    public int totalTuples() {
        return child1.totalTuples() + child2.totalTuples();
    }
    @Override
    public int numTuples() {
        return this.child1.numTuples() + child2.numTuples();
    }

}
