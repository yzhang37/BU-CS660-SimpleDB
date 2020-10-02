package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private ArrayList<TDItem> tdItems;
    private boolean has_names = false;
    private int pre_calced_Size = -1;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return this.tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length < 1) {
            throw new IllegalArgumentException("typeAr must contains at least 1 entry.");
        }
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("typeAr and fieldAr must have the same length.");
        }
        this.tdItems = new ArrayList<>();
        int len = typeAr.length;
        for (int i = 0; i < len; ++i) {
            TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
            this.tdItems.add(tdItem);
        }
        this.PreInitialize();
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        if (typeAr.length < 1) {
            throw new IllegalArgumentException("typeAr must contains at least 1 entry.");
        }
        this.tdItems = new ArrayList<>();
        for (Type type : typeAr) {
            TDItem tdItem = new TDItem(type, null);
            this.tdItems.add(tdItem);
        }
        this.PreInitialize();
    }

    /**
     * Constructor only used in merge function.
     * @param list1 array from the 1st TupleDesc object.
     * @param list2 array from the 2nd TupleDesc object.
     */
    private TupleDesc(ArrayList<TDItem> list1, ArrayList<TDItem> list2) {
        this.tdItems = new ArrayList<>();
        this.tdItems.addAll(list1);
        this.tdItems.addAll(list2);
        this.PreInitialize();
    }

    /**
     * Utility used to initialize some fields useful to some member functions.
     */
    private void PreInitialize() {
        this.pre_calced_Size = 0;
        for (TDItem tdItem: this.tdItems) {
            Type type = tdItem.fieldType;
            this.pre_calced_Size += type.getLen();
            if (!this.has_names && tdItem.fieldName != null) {
                this.has_names = true;
            }
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return this.tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return this.tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (!this.has_names) {
            throw new NoSuchElementException();
        }
        int count = 0;
        for (TDItem tdItem : this.tdItems) {
            if (tdItem.fieldName.equals(name)) {
                return count;
            }
            ++count;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        assert(this.pre_calced_Size != -1);
        return this.pre_calced_Size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        return new TupleDesc(td1.tdItems, td2.tdItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o instanceof TupleDesc) {
            if (o == this) {
                return true;
            } else{
                TupleDesc tdo = (TupleDesc) o;
                if (this.getSize() == tdo.getSize() &&
                    this.tdItems.size() == tdo.tdItems.size()) {
                    for (int i = 0; i < this.tdItems.size(); ++i) {
                        if (this.tdItems.get(i).fieldType != tdo.tdItems.get(i).fieldType) {
                            return false;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
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
        StringBuilder sb = new StringBuilder();
        for (TDItem tdItem : this.tdItems) {
            sb.append(tdItem.toString());
        }
        return sb.toString();
    }
}
