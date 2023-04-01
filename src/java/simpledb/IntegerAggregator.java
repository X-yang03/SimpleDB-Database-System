package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;

    private Map<Field,Integer> groupMap;

    private Map<Field , Integer[]> avgMap;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        this.groupMap = new HashMap<>();
        this.avgMap = new HashMap<>();
        if(this.gbfield == NO_GROUPING){
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
        }
        else if(what.equals(Op.SUM_COUNT)){
            this.td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE, Type.INT_TYPE}, new String[]{"GroupVal","Sum","Count"});
        }
        else{
            this.td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField field = (IntField) tup.getField(this.afield);  //IntField implemented the Field interface
        if(field == null){
            return;
        }
        Field gbfield;
        if(this.gbfield == NO_GROUPING){
           gbfield = null;
        }else{
            gbfield = tup.getField(this.gbfield);   //gbfield is used to group tuples
        }

        int value = field.getValue();
        if(gbfield != null && gbfield.getType() != this.gbfieldtype){
            throw new IllegalArgumentException();
        }
        switch (this.what){
            case MIN:
                if(!this.groupMap.containsKey(gbfield))
                    this.groupMap.put(gbfield,value);
                else
                    this.groupMap.put(gbfield,Math.min(this.groupMap.get(gbfield),value));
                break;
            case MAX:
                if(!this.groupMap.containsKey(gbfield))
                    this.groupMap.put(gbfield,value);
                else
                    this.groupMap.put(gbfield,Math.max(this.groupMap.get(gbfield),value));
                break;
            case SUM:
                if(!this.groupMap.containsKey(gbfield))
                    this.groupMap.put(gbfield,value);
                else
                    this.groupMap.put(gbfield,this.groupMap.get(gbfield)+value);
                break;
            case COUNT:
                if(!this.groupMap.containsKey(gbfield))
                    this.groupMap.put(gbfield,1);
                else
                    this.groupMap.put(gbfield,this.groupMap.get(gbfield) + 1);
                break;
           /* case SC_AVG:
                IntField cntField = null;
                if(gbfield == null)
                    cntField = (IntField) tup.getField(1);
                else
                    cntField = (IntField) tup.getField(2);
                int cntValue = cntField.getValue();
                if(!this.groupMap.containsKey(gbfield)){
                    this.groupMap.put(gbfield,value);
                    this.countMap.put(gbfield,cntValue);
                }else{
                    this.groupMap.put(gbfield, this.groupMap.get(gbfield)+value);
                    this.countMap.put(gbfield,this.countMap.get(gbfield)+cntValue);
                }*/
            case SUM_COUNT: //using ArrayList we are able to know the values and its length, thus we could get the avg value
            case AVG:
                if(!this.avgMap.containsKey(gbfield)){

                    avgMap.put(gbfield, new Integer[]{1, value});
                    groupMap.put(gbfield,value);
                }else{
                    int oldCnt = avgMap.get(gbfield)[0];
                    int oldSum = avgMap.get(gbfield)[1];
                    avgMap.put(gbfield, new Integer[]{oldCnt+1, oldSum + value});
                    groupMap.put(gbfield,(oldSum+value)/(oldCnt+1));
                }
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
            for (Map.Entry<Field, Integer> g2a : groupMap.entrySet()) {
                Tuple tp = new Tuple(td);
                if (gbfield == NO_GROUPING) {
                    tp.setField(0, new IntField(g2a.getValue()));
                } else {
                    tp.setField(0, g2a.getKey());
                    tp.setField(1, new IntField(g2a.getValue()));
                }
                tuples.add(tp);
            }

        return new TupleIterator(td, tuples);
    }


}
