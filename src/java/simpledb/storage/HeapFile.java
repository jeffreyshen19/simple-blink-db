package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    
    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(this.f, "rw");
        } catch (FileNotFoundException e2) {
            throw new IllegalArgumentException("");
        }
        
        byte[] data = new byte[BufferPool.getPageSize()];
        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        try {
            raf.seek(offset);
            raf.read(data, 0, data.length);
            raf.close();
        } catch (IOException e1) {
            throw new IllegalArgumentException("Page does not exist in file");
        }
 
        try {
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Page does not exist in file");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pgNo = page.getId().getPageNumber();

        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(this.f, "rw");
        } catch (FileNotFoundException e2) {
            throw new IllegalArgumentException("");
        }
        
        raf.seek(pgNo * BufferPool.getPageSize());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
        int n = numPages();
        HeapPage page;
        
        for(int i = 0; i < n; i++) { // Go through all pages, find one with an empty slot 
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (page.getNumUnusedSlots() > 0) {
                page.insertTuple(t);
                return Arrays.asList(page);
            }
        }
        
        // Create a new page
        page = new HeapPage(new HeapPageId(this.getId(), n), HeapPage.createEmptyPageData());
        page.insertTuple(t);
        writePage(page);
        
        return Arrays.asList(page);
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage page;
        try {
            page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        }
        catch (IllegalArgumentException e) {
            throw new DbException("Tuple does not belong to this file");
        }
        
        page.deleteTuple(t);
        
        return Arrays.asList(page);
    }
    
    /**
     * Helper class to implement Iterator.
     */
    class HeapFileIterator implements DbFileIterator{
        
        private int tableId;
        private int numPages;
        private int pgNo;
        private boolean opened = false;
        private HeapPage page;
        private TransactionId tid;
        private Iterator<Tuple> pageIterator;
        
        public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
            this.tid = tid;
            this.tableId = tableId;
            this.numPages = numPages;
            this.pgNo = 0;
        }
        
        private void loadPage() throws TransactionAbortedException, DbException {
            this.page = (HeapPage) (Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo), Permissions.READ_ONLY));
            this.pageIterator = this.page.iterator();
        }
        
        public void open() 
                throws DbException, TransactionAbortedException{
            this.opened = true;
            this.loadPage();
        }

        public boolean hasNext()
                throws DbException, TransactionAbortedException{
            if(!opened) return false;
            
            // If this page still has more
            if(pageIterator.hasNext()) return true;
            
            // Check if all remaining pages have values.
            for(int i = pgNo + 1; i < this.numPages; i++) {
                HeapPage nextPage = (HeapPage) (Database.getBufferPool().getPage(tid, new HeapPageId(tableId, i), Permissions.READ_ONLY));
                if (nextPage.iterator().hasNext()) return true;
            }
            
            return false;
        }
        
        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException{
            if(!hasNext()) throw new NoSuchElementException("no more tuples");
            
            if(pageIterator.hasNext()) return pageIterator.next();
            else{ 
                do {
                    this.pgNo++;
                    this.loadPage();
                }
                while(!pageIterator.hasNext());
                return pageIterator.next();
            }
        }

        public void rewind() throws DbException, TransactionAbortedException{
            this.pgNo = 0;
            this.loadPage();
        }

        public void close() {
            // TODO
            this.opened = false;
        }
    }
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this.getId(), this.numPages());
    }

}

