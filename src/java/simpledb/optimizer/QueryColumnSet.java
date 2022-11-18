package simpledb.optimizer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Query Column Set (QCS): 
 * Sets of columns used by queries in WHERE, GROUP BY,
 * and HAVING clauses
 */
public class QueryColumnSet {
    
    private Set<String> columns;
    
    public QueryColumnSet(String... c) {
        columns = new HashSet<>(Arrays.asList(c));
    }
    
    @Override
    public int hashCode() {
        int result = 0;
        for(String column : columns) result += column.hashCode();
        return result;
    }
        
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof QueryColumnSet)) return false;
        
        QueryColumnSet other = (QueryColumnSet) o;
        return this.columns.equals(other.columns);
    }
}
