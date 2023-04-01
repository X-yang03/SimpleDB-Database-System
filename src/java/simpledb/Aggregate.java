package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */

    private OpIterator it;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private TupleDesc child_td;
    private TupleDesc td;
    private Type gbfieldtype;
    private Type afieldtype;
    private OpIterator opIterator;
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.it=child;
        this.afield= afield;
        this.gfield = gfield;
        this.aop = aop;
        child_td = child.getTupleDesc();
        afieldtype = child_td.getFieldType(afield);
        gbfieldtype = gfield == Aggregator.NO_GROUPING? null : child_td.getFieldType(gfield);
        if(afieldtype == Type.INT_TYPE){
            aggregator = new IntegerAggregator(gfield, gbfieldtype, afield, aop);
        }else if(afieldtype == Type.STRING_TYPE){
            aggregator = new StringAggregator(gfield, gbfieldtype, afield, aop);
        }
        Type[] types;
        String[] names;
        String aname = child_td.getFieldName(afield);
        if(gfield == Aggregator.NO_GROUPING){
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{aname};
        }else{
            types = new Type[]{gbfieldtype, Type.INT_TYPE};
            names = new String[]{child_td.getFieldName(gfield),aname};
        }
        this.td = new TupleDesc(types, names);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	if(gfield == Aggregator.NO_GROUPING)return null;
    return it.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return it.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        super.open();
        it.open();
        while(it.hasNext()){
            aggregator.mergeTupleIntoGroup(it.next());
        }
        it.close();
        opIterator = aggregator.iterator();
        opIterator.open();

    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        while (opIterator.hasNext()){
            return opIterator.next();
        }
	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        return td;

    }

    public void close() {
	// some code goes here
        super.close();
        opIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
        return new OpIterator[]{this.it};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        this.it = children[0];
        Type fieldType = this.it.getTupleDesc().getFieldType(afield);
        Type groupType = gfield==Aggregator.NO_GROUPING? null : this.it.getTupleDesc().getFieldType(gfield);
        if(fieldType == Type.INT_TYPE){
            this.aggregator=new IntegerAggregator(gfield, groupType,afield,aop);
        }else{
            this.aggregator=new StringAggregator(gfield, groupType, afield,aop);
        }
    }
    
}
