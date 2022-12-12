package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    
    private int[] bucketArr;
    private int buckets;
    private int min;
    private int max;
    private int ntups; 
    private double width; // bucket width

    private double sum = 0.0; 
    private double squaredSum = 0.0;
    private double cubedSum = 0.0;

    private double mean = 0.0;
    private double sd = 0.0;
    private double skew = 0.0;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.bucketArr = new int[buckets];
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.ntups = 0;
        this.width = (double) (max - min) / buckets;
    }
    
    
    /**
     * Get the index corresponding to the bucket of v
     * @param v Value
     * @return index of bucket
     */
    private int getBucket(int v) {
        int i = (int) ((v - min) / width); // bucket v goes into 
        if(i == buckets) i--; // max should map to the last bucket
        return i;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int i = getBucket(v);
        this.bucketArr[i]++;
        this.ntups++;

        this.sum += v;
        this.squaredSum += Math.pow(v, 2.0);
        this.cubedSum += Math.pow(v, 3.0);

        this.mean = sum / ntups;
        this.sd = Math.sqrt((squaredSum - Math.pow(mean, 2)) / ntups);
        this.skew = (cubedSum - 3 * mean * squaredSum + 2 * Math.pow(mean, 3)) / (ntups * Math.pow(sd, 3));
    }

    /**
     * @return the skewness of the histogram
     */
    public double getSkew() {
        return skew;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int i = getBucket(v);
        double selectivity;
        
        switch(op) {
            case EQUALS:
                if(v < min || v > max) return 0.0;
                return (double) bucketArr[i] / ntups;
            case NOT_EQUALS: 
                return 1 - this.estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN: 
                if(v < min) return 1.0;
                else if(v > max) return 0.0;
                // Right portion of the current bucket
                selectivity = (double) bucketArr[i] * (min + (i + 1) * width - v) / (width * ntups); 
                // All buckets to the right
                for(int j = i + 1; j < buckets; j++) selectivity += (double) bucketArr[j] / ntups;
                return selectivity;
            case GREATER_THAN_OR_EQ: 
                if(v < min) return 1.0;
                else if(v > max) return 0.0;
                selectivity = 0;
                for(int j = i; j < buckets; j++) selectivity += (double) bucketArr[j] / ntups;
                return selectivity;
            case LESS_THAN:
                return 1 - this.estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case LESS_THAN_OR_EQ:
                if(v < min) return 0.0;
                else if(v > max) return 1.0;
                selectivity = 0;
                for(int j = 0; j <= i; j++) selectivity += (double) bucketArr[j] / ntups;
                return selectivity;
            default:
                return -1;
        }
 
    }
    

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String result = "";
        
        for(int i = 0; i < buckets; i++) {
            result += bucketArr[i] + " (" + (i * width + min) +  ", " + ((i + 1) * width + min) +  ") ";
        }
        
        return result;
    }
}
