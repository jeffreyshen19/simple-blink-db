package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleDesc.TDItem;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private boolean opened = false;
    
    private DbFile db;
    private DbFileIterator iterator;
    private TupleDesc td;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.setDbFile();
    }

    /**
     * @return return the table name of the table the operator scans. This should
     *         be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return this.tableAlias;
    }
    
    /**
     * Initialize fields db and iterator, based off the table specified by this.tableid
     */
    private void setDbFile() {
        this.db = Database.getCatalog().getDatabaseFile(this.tableid);
        this.iterator = db.iterator(this.tid);
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.td = null;
        this.setDbFile();
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.opened = true;
        this.iterator.open();
    }
    
    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {        
        if(this.td == null) { // We only want to generate the TupleDesc once per table
            TupleDesc td = this.db.getTupleDesc();
            Type[] types = new Type[td.numFields()];
            String[] fields = new String[td.numFields()];
            
            int i = 0;
            for (Iterator<TDItem> iterator = td.iterator(); iterator.hasNext(); i++){
                TDItem item = iterator.next();
                types[i] = item.fieldType;
                fields[i] = item.fieldName == null ? null : this.tableAlias + "." + item.fieldName;
            }
            
            this.td = new TupleDesc(types, fields);
        }
        
        return this.td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if(!this.opened) throw new IllegalStateException("SeqScan not opened");
        return this.iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Tuple nextTuple = iterator.next();
        nextTuple.resetTupleDesc(this.getTupleDesc());
        return nextTuple;
    }

    public void close() {
        this.opened = false;
        this.iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.iterator.rewind();
    }
}
