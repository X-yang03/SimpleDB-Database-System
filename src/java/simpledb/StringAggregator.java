package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;

    private Map<Field, Integer> countMap;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(!(what.equals(Op.COUNT))){
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        countMap = new HashMap<>();
        if(this.gbfield == NO_GROUPING){
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
        }
        else{
            this.td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField afield = (StringField) tup.getField(this.afield);
        Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        String value = afield.getValue();
        if(gbfield != null && gbfield.getType()!= this.gbfieldtype){
            throw new IllegalArgumentException();
        }
        if(!this.countMap.containsKey(gbfield)){  //first time
            this.countMap.put(gbfield, 1);
        }else{
            this.countMap.put(gbfield, countMap.get(gbfield) + 1);

        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");
        ArrayList<Tuple> tuples = new ArrayList<>();
        for(Map.Entry<Field, Integer> a : countMap.entrySet())
        {
            Tuple tp = new Tuple(td);
            if(gbfield == NO_GROUPING){
                tp.setField(0,new IntField(a.getValue()));
            }else{
                tp.setField(0,a.getKey());
                tp.setField(1,new IntField(a.getValue()));
            }
            tuples.add(tp);
        }
        return new TupleIterator(td,tuples);
    }

}
