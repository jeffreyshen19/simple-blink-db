package simpledb.storage;

import java.util.List;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class SampleIterator implements DbFileIterator {
    private int maxSample;
    private boolean opened;
    private int sampleIndex = 0;
    private TransactionId tid;
    private DbFile currSampFile;
    private DbFileIterator currSampFileIterator;
    private List<DbFile> samples;

    public SampleIterator(List<DbFile> samples, int maxSample, TransactionId tid) {
        this.maxSample = maxSample;
        this.tid = tid;
        this.samples = samples;
        currSampFile = samples.get(sampleIndex);
        currSampFileIterator = currSampFile.iterator(this.tid);
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException{
        if (!opened) return false;
        if (currSampFileIterator.hasNext()) return true;
        //if sampleIndex = maxSample, then was covered by checking if curriterator has next
        return sampleIndex < maxSample;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException {
        if (!hasNext()) throw new NoSuchElementException("No more tuples");
        
        if (currSampFileIterator.hasNext()) {
            return currSampFileIterator.next();
        }
        if (sampleIndex == samples.size() - 1) throw new NoSuchElementException("No more tuples");

        currSampFileIterator.close();
        sampleIndex++;
        currSampFile = samples.get(sampleIndex);
        currSampFileIterator = currSampFile.iterator(this.tid);
        currSampFileIterator.open();

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
        this.sampleIndex = 0;
        currSampFile = samples.get(sampleIndex);
        currSampFileIterator = currSampFile.iterator(tid);
    }
}