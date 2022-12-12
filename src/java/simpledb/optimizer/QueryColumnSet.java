package simpledb.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import simpledb.execution.Aggregate;
import simpledb.execution.Aggregator;
import simpledb.execution.Filter;
import simpledb.execution.OpIterator;
import simpledb.execution.Operator;
import simpledb.execution.SeqScanSample;
import simpledb.transaction.TransactionId;

/**
 * Query Column Set (QCS): 
 * Sets of columns used by queries in WHERE, GROUP BY,
 * and HAVING clauses
 */
public class QueryColumnSet {
    
    private Set<Integer> columns;
    
    /**
     * Get the columns used by queries in WHERE, GROUP BY, and HAVING clauses
     * @param query
     * @return
     */
    private Set<Integer> getColumnsFromQuery(OpIterator query){
        Set<Integer> columns = new HashSet<Integer>();
        if (query instanceof Operator) { // JOIN, FILTER, AGGREGATE
            Operator operator = (Operator) query;
            OpIterator[] children = operator.getChildren();
            
            // Add columns from this query
            if(query instanceof Filter) { // WHERE
                Filter filter = (Filter) query;
                int whereField = filter.getPredicate().getField();
                columns.add(whereField);
            }
            else if(query instanceof Aggregate) { // GROUP BY / HAVING
                Aggregate aggregate = (Aggregate) query;
                int groupField = aggregate.aggregateField();
                if(groupField != Aggregator.NO_GROUPING) columns.add(groupField);
            }
            
            // Add columns from children
            for(int i = 0; i < children.length; i++) {
                for(Integer column : getColumnsFromQuery(children[i])) {
                    columns.add(column);
                }
            }
        }
        
        // No columns for SEQSCAN
        return columns; 
    }
    
    public QueryColumnSet(OpIterator query) {
        columns = getColumnsFromQuery(query);
    }
    
    public QueryColumnSet(Integer... c) {
        columns = new HashSet<Integer>(Arrays.asList(c));
    }
    
    public Set<Integer> getColumns() {
    	Set<Integer> copyColumns = new HashSet<>();
    	copyColumns.addAll(columns);
    	return copyColumns;
    }
    
    public int getNumCols() {
    	return columns.size();
    }
    
    @Override
    public int hashCode() {
        int result = 0;
        for(Integer column : columns) result += column.hashCode();
        return result;
    }
        
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof QueryColumnSet)) return false;
        
        QueryColumnSet other = (QueryColumnSet) o;
        return this.columns.equals(other.columns);
    }
}
