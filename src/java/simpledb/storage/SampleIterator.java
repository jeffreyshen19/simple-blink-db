package simpledb.storage;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class SampleIterator implements DbFileIterator {
    private int numRows;
    private boolean opened = false;
    private int index = 0;
    private TransactionId tid;
    private int tableId;
    private Iterator<Tuple> pageIterator;
    private HeapPage page;
    private int pgNo = 0;
    private int numPages;

    public SampleIterator(int tableId, TransactionId tid, int numPages, int numRows) {
        this.numRows = numRows;
        this.tid = tid;
        this.tableId = tableId;
        this.numPages = numPages;
    }

    private void loadPage() throws TransactionAbortedException, DbException {
        this.page = (HeapPage) (Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo), Permissions.READ_ONLY));
        this.pageIterator = this.page.iterator();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException{
        this.opened = true;
        this.loadPage();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException{
        if (!opened) return false;
        //check to see if reached max num of tuples allowed
        if (index >= numRows) return false;
        // check if curr page has more
        if (pageIterator.hasNext()) return true;
        // check if other pages have values to spit
        for(int i = this.pgNo + 1; i < this.numPages; i++) {
            HeapPage nextPage = (HeapPage) (Database.getBufferPool().getPage(tid, new HeapPageId(tableId, i), Permissions.READ_ONLY));
            if (nextPage.iterator().hasNext()) return true;
        }

        //if this happens then idk what went wrong bro
        //either numRows was way too big or something 
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException {
        if (!hasNext()) throw new NoSuchElementException("No more tuples");
        
        if (pageIterator.hasNext()) {
            index++;
            return pageIterator.next();
        }
        if (index >= numRows) throw new NoSuchElementException("No more tuples");

        do {
            this.pgNo++;
            this.loadPage();
        } while(!pageIterator.hasNext());

        index++;
        return pageIterator.next();
    }



    @Override
    public void close() {
        this.opened = false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException{
        this.index = 0;
        this.pgNo = 0;
        this.loadPage();
    }
}