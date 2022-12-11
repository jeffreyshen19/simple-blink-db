package simpledb.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Object, ArrayList<Tuple>> groups; // Maps group field value to group
    private int nTups;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if(what != Op.COUNT) throw new IllegalArgumentException("Operator must be COUNT");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        
        this.groups = new HashMap<>();
        this.nTups = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Object fieldValue;
        
        if(gbfieldtype == Type.INT_TYPE) fieldValue = ((IntField) tup.getField(gbfield)).getValue();
        else if(gbfieldtype == Type.STRING_TYPE) fieldValue = ((StringField) tup.getField(gbfield)).getValue();
        else fieldValue = null; // No grouping
        
        if(!groups.containsKey(fieldValue)) {
            groups.put(fieldValue, new ArrayList<Tuple>());
        }
        
        groups.get(fieldValue).add(tup);
        this.nTups++;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new AggregatorIterator(groups, gbfieldtype, gbfield, afield, what);
    }

    @Override
    public int getNumTups() {
        return nTups;
    }

}
