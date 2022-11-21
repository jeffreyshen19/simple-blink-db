package simpledb.storage;

import java.util.List;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * SampleWrapper is a wrapper for multiple DbFiles to store different sized samples in one object
 * The samples inside are part of the same sample family
 * Samples are stored as separate DBFiles, 
 * similarly to BlinkDB's description of samples building on top of each other
 */
public class SampleWrapper {
    final List<Integer> sampleSizes;
    final TupleDesc stratifiedColumns;
    final List<DbFile> samples;


    /**
     * Create SampleWrapper for a table
     * 
     * @param sampleSizes - list of sample sizes - must be in increasing order
     * @param stratifiedColumns - columns to stratify sample on
     *                       if stratifiedColumns is null, then this is a uniform sample
     * @param origFile - the file from which the samples are being created from 
     */
    public SampleWrapper(List<Integer> sampleSizes, TupleDesc stratifiedColumns, DbFile origFile) {
        this.sampleSizes = sampleSizes;
        this.stratifiedColumns = stratifiedColumns;
        //this.samples is made by the two funcs below
        if (this.stratifiedColumns == null) {
            createUniformSamples(origFile);
        } else {
            createStratifiedSamples(origFile);
        }
    }
    

    private void createUniformSamples(DbFile origFile) {
        return;
    }

    private void createStratifiedSamples(DbFile origFile) {
        return;   
    }

    public boolean isStratified() {
        return this.stratifiedColumns == null;
    }

    class SampleIterator implements DbFileIterator {
        private int sampleSize;
        private boolean opened;
        private int i = 0;
        private int sampleIndex = 0;
        private TransactionId tid;
        private DbFile currSampFile;
        private DbFileIterator currSampFileIterator;

        public SampleIterator(int sampleSize, TransactionId tid) {
            this.sampleSize = sampleSize;
            this.tid = tid;
            currSampFile = samples.get(sampleIndex);
            currSampFileIterator = currSampFile.iterator(this.tid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException{
            if (!opened) return false;
            if (i > sampleSize) return false;
            if (currSampFileIterator.hasNext()) return true;
            return sampleIndex < samples.size();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException {
            if (!hasNext()) throw new NoSuchElementException("No more tuples");
            
            if (currSampFileIterator.hasNext()) {
                this.i++;
                return currSampFileIterator.next();
            }
            if (sampleIndex == samples.size() - 1) throw new NoSuchElementException("No more tuples");

            currSampFileIterator.close();
            sampleIndex++;
            currSampFile = samples.get(sampleIndex);
            currSampFileIterator = currSampFile.iterator(this.tid);
            currSampFileIterator.open();

            this.i++;
            return currSampFileIterator.next();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException{
            this.opened = true;
            currSampFileIterator.open();
        }

        @Override
        public void close() {
            this.opened = false;
            currSampFileIterator.close();
        }

        @Override
        public void rewind() {
            this.i = 0;
            this.sampleIndex = 0;
            currSampFile = samples.get(sampleIndex);
            currSampFileIterator = currSampFile.iterator(tid);
        }
    }

    /**
     * Create an iterator for this sample
     * @param sampleSize - the size sample to be pulled from this samplewrapper
     * @param tid - transaction id
     * @return - an iterator that will return sampleSize number of tuples 
     */
    public SampleIterator iterator(int sampleSize, TransactionId tid) {
        return new SampleIterator(sampleSize, tid);
    }
    
}
