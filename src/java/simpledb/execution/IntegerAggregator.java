package simpledb.execution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.HeapPage;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Object, ArrayList<Tuple>> groups; // Maps group field value to group. If no grouping, all tuples are in group with key null

    private double mean; 
    private double variance; 
    private int nTups = 0;
    
    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        
        this.groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Object fieldValue;
        
        if(gbfieldtype == Type.INT_TYPE) {
            fieldValue = ((IntField) tup.getField(gbfield)).getValue();
            double val = (double) fieldValue;
            // calculate mean and variance if the field type is INT
            if (this.nTups < 1) {
                this.mean = val;
                this.variance = 0;
            } else {
                double newMean = this.mean + (val - this.mean) / this.nTups;
                this.variance = this.variance + (val - this.mean) * (val - newMean);
                this.mean = newMean;
            }
        }
        else if(gbfieldtype == Type.STRING_TYPE) fieldValue = ((StringField) tup.getField(gbfield)).getValue();
        else fieldValue = null; // No grouping
        
        if(!groups.containsKey(fieldValue)) {
            groups.put(fieldValue, new ArrayList<Tuple>());
        }
        
        groups.get(fieldValue).add(tup);
        this.nTups++;
    }

    /**
     * @return Sample Mean
     */
    public double getSampleVariance() {
        return this.variance;
    }

    /**
     * @return number of tuples selected by query 
     */
    @Override
    public int getNumTups() {
        return this.nTups;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new AggregatorIterator(groups, gbfieldtype, gbfield, afield, what);
    }
    
}
