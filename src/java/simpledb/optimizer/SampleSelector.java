package simpledb.optimizer;

import java.util.List;

import simpledb.common.Database;
import simpledb.execution.Query;
import simpledb.storage.SampleFamily;

public class SampleSelector {
    
    /**
     * TODO: Victor 
     * Given a QueryColumnSet q_j, return the sample family to choose 
     * @param qcs
     * @return SampleFamily
     */
    public SampleFamily selectSample(QueryColumnSet qcs) {
        List<SampleFamily> sampleFamilies = Database.getSampleFamilies();
        throw new UnsupportedOperationException();
    }
    
    /**
     * TODO: Jeffrey
     * Given a sampleFamily and error target, return the estimated size of the sample satisfying this target
     * @param sampleFamily
     * @param q 
     * @param errorTarget
     * @return n, the number of rows to read from the sample
     */
    public int selectSampleSizeError(SampleFamily sampleFamily, Query q, double errorTarget) {
        return 0;
    }
    
    /**
     * TODO: Yun
     * Given a sampleFamily and latency target, return the estimated size of the sample satisfying this target
     * @param sampleFamily
     * @param q 
     * @param latencyTarget
     * @return n, the number of rows to read from the sample
     */
    public int selectSampleSizeLatency(SampleFamily sampleFamily, Query q, double latencyTarget) {
        return 0;
    }
    
}
