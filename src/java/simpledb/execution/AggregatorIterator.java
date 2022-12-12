package simpledb.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

public class AggregatorIterator implements OpIterator{
    
    private Iterator mapIterator;
    private Map<Object, ArrayList<Tuple>> groups;
    private boolean opened = false;
    private int afield;
    private int gbfield;
    private Op what;
    private Type gbfieldtype;
    private Set valid;
    private int totalTuples = 0;
    private int numTuples = 0;
    
    public AggregatorIterator(Map<Object, ArrayList<Tuple>> groups, Type gbfieldtype, int gbfield, int afield, Op what) {
        this.mapIterator = groups.keySet().iterator();
        this.groups = groups;
        this.afield = afield;
        this.gbfield = gbfield;
        this.what = what;
        this.gbfieldtype = gbfieldtype;
    }
    
    public void open()
            throws DbException, TransactionAbortedException{
        this.opened = true;
    }

    public boolean hasNext() throws DbException, TransactionAbortedException{
        if(!this.opened) throw new IllegalStateException();
        return this.mapIterator.hasNext();
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
        if(!this.opened) throw new IllegalStateException();
        if(!this.hasNext()) throw new NoSuchElementException();
        
        Object key = mapIterator.next();
        ArrayList<Tuple> group = groups.get(key);
        Integer aggregateVal = null;
        int total = 0;
        
        for(Tuple tuple : group) {
            int fieldValue;
            
            switch(what) {
                case MIN: 
                    fieldValue = ((IntField) tuple.getField(afield)).getValue();
                    if(aggregateVal == null) aggregateVal = fieldValue;
                    else aggregateVal = Math.min(aggregateVal, fieldValue);
                    break;
                case MAX: 
                    fieldValue = ((IntField) tuple.getField(afield)).getValue();
                    if(aggregateVal == null) aggregateVal = fieldValue;
                    else aggregateVal = Math.max(aggregateVal, fieldValue);
                    break;
                case SUM: 
                    fieldValue = ((IntField) tuple.getField(afield)).getValue();
                    if(aggregateVal == null) aggregateVal = fieldValue;
                    else aggregateVal += fieldValue;
                    break;
                case AVG: 
                    fieldValue = ((IntField) tuple.getField(afield)).getValue();
                    if(aggregateVal == null) aggregateVal = fieldValue;
                    else aggregateVal += fieldValue;
                    total++;
                    break;
                case COUNT: 
                    if(aggregateVal == null) aggregateVal = 0;
                    aggregateVal++;
                    break;
            }
        }
        
        if(what == Op.AVG) aggregateVal /= total;
        
        // Return (group field value, aggregate value)
        Tuple next = new Tuple(getTupleDesc());
        
        if(gbfield == Aggregator.NO_GROUPING) next.setField(0, new IntField(aggregateVal));
        else {
            switch(gbfieldtype) {
                case INT_TYPE: 
                    next.setField(0, new IntField((int) key));
                    break;
                case STRING_TYPE: 
                    next.setField(0, new StringField((String) key, ((String) key).length()));
                    break;
            }
            next.setField(1, new IntField(aggregateVal));
        }
        
        return next;
    }
    
    public void rewind() throws DbException, TransactionAbortedException{
        if(!this.opened) throw new IllegalStateException();
        mapIterator = groups.keySet().iterator();
        
    }

    public TupleDesc getTupleDesc() {
        Type[] typeAr;
        
        if(gbfield == Aggregator.NO_GROUPING) typeAr = new Type[]{Type.INT_TYPE};
        else typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
             
        return new TupleDesc(typeAr);
    }
    
    public void close() {
        this.opened = false;
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