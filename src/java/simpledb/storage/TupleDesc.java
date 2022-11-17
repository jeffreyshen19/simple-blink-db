package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    
    /**
     * Stores each of the items (field name, types) in the TupleDesc
     */
    private TDItem[] tdItems;
  

    /**
     * A help class to facilitate organizing the information of each field
     */
    
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldType + "(" + fieldName + ")";
        }
        
        public boolean equals(Object o) {
            if (!(o instanceof TDItem)) return false;
            TDItem other = (TDItem) o;
            
            return (this.fieldName == null ? other.fieldName == null : this.fieldName.equals(other.fieldName)) && this.fieldType == other.fieldType;
        }
    }
    

    /**
     * @return An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return Arrays.stream(this.tdItems).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.tdItems = new TDItem[typeAr.length];
        
        for(int i = 0; i < typeAr.length; i++) {
            Type type = typeAr[i];
            String fieldName = fieldAr[i];
            this.tdItems[i] = new TDItem(type, fieldName);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i < 0 || i >= this.numFields()) throw new NoSuchElementException("i is not a valid field reference");
        return this.tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i < 0 || i >= this.numFields()) throw new NoSuchElementException("i is not a valid field reference");
        return this.tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        if(name == null) throw new NoSuchElementException("name cannot be null");
        
        for(int i = 0; i < this.numFields(); i++) {
            if(name.equals(this.getFieldName(i))) return i;
            
        }
        throw new NoSuchElementException("no field with a matching name found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for(int i = 0; i < this.numFields(); i++) {
            size += this.getFieldType(i).getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        String[] mergedFields = new String[td1.numFields() + td2.numFields()];
        Type[] mergedTypes = new Type[td1.numFields() + td2.numFields()];
        
        for(int i = 0; i < td1.numFields(); i++) {
            mergedFields[i] = td1.tdItems[i].fieldName;
            mergedTypes[i] = td1.tdItems[i].fieldType;
        }
        
        for(int i = 0; i < td2.numFields(); i++) {
            mergedFields[i + td1.numFields()] = td2.tdItems[i].fieldName;
            mergedTypes[i + td1.numFields()] = td2.tdItems[i].fieldType;
        }
        
        return new TupleDesc(mergedTypes, mergedFields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) return false;
        
        TupleDesc other = (TupleDesc) o;
        if(this.numFields() != other.numFields()) return false;
        for(int i = 0; i < this.numFields(); i++) {
            if(!this.tdItems[i].equals(other.tdItems[i])) return false;
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String string = "";
        
        for(int i = 0; i < this.numFields(); i++) {
            string += (i == 0 ? "" : ", ") + this.tdItems[i].toString(); 
        }
        
        return string;
    }
}
