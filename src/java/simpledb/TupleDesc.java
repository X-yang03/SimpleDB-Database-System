package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    private ArrayList<TDItem> TdArray= new ArrayList<>();
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
        // some code goes here
        //return null;
       return TdArray.iterator();
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
        // some code goes here
        if(typeAr==null){
               // throw new IllegalArgumentException("Must contain at least one entry");
        }
        else {
            for (int i = 0; i < typeAr.length; i++) {
                TdArray.add(new TDItem(typeAr[i], fieldAr[i]));
            }
        }
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
        // some code goes here
        if(typeAr==null) {
           // throw new IllegalArgumentException();
        }
        else {
            for (int i = 0; i < typeAr.length; i++) {
                TdArray.add(new TDItem(typeAr[i], null));
            }
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TdArray.size();
        //return 0;
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
        // some code goes here
        if(i>=TdArray.size()||i<0){
            throw new NoSuchElementException();
        }
        return TdArray.get(i).fieldName;
        //return null;
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
        // some code goes here
        if(i>=TdArray.size()||i<0){
            throw new NoSuchElementException();
        }
        return TdArray.get(i).fieldType;
        //return null;
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
        // some code goes here
            for (int i = 0; i < TdArray.size(); i++) {
               if(getFieldName(i)==null)
                   continue;
               else {
                   if (getFieldName(i).equals(name))
                       return i;
               }
            }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size=0;
        for(int i=0;i<TdArray.size();i++)
            size+=getFieldType(i).getLen();
        return size;
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
        // some code goes here
        TupleDesc tmp=new TupleDesc(null,null);
        tmp.TdArray.addAll(td1.TdArray);
        tmp.TdArray.addAll(td2.TdArray);
        return tmp;
       // return null;
    }

    @Override
    public boolean equals(Object o) {
        if (!this.getClass().isInstance(o)) {
            return false;
        }
        TupleDesc desc = (TupleDesc) o;
        int len = this.TdArray.size();
        List<TDItem> tdItems = desc.TdArray;
        int size = tdItems.size();
        if (len != size) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            TDItem item = this.TdArray.get(i);
            TDItem tdItem = tdItems.get(i);
            if (!item.fieldType.equals(tdItem.fieldType)) {
                return false;
            }
        }
        return true;
    }



    public int hashCode() {
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
        // some code goes here
        String info="";
        for(int i=0;i<TdArray.size();i++){
            info =info+ getFieldType(i)+'('+getFieldName(i)+')';
            if(i!=TdArray.size()-1)
                info=info+',';
        }

        return info;
    }
}
