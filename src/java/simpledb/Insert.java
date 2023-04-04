package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private int count;      //the number of the affected tuples
    private TupleDesc returnTP;   //with only one field
    private boolean isAccessed;
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))){
            throw new DbException("TupleDesc do not match!");
        }
        this.t=t;
        this.child =  child;
        this.tableId = tableId;
        count = -1;   //in the beginning, count is set as -1, meaning this inset operator is not used
        isAccessed = false;
        returnTP = new TupleDesc(new Type[]{Type.INT_TYPE} , new String[]{null});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return returnTP;

    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        count = 0;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        count = -1;
        isAccessed = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        count = 0;
        isAccessed = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if ( isAccessed )
            return null;
        isAccessed = true;
        while (this.child.hasNext()) {
            Tuple t = this.child.next();
            try {
                Database.getBufferPool().insertTuple(this.t, this.tableId, t);
                this.count++;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
        Tuple tuple = new Tuple(this.returnTP);
        tuple.setField(0, new IntField(this.count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
       return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
