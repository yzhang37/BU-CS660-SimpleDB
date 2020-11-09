package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private boolean used = true;

    private TupleDesc cntTD;

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
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableId = tableId;

        // check if child's TD differs from that of the table
        if (!Database.getCatalog().getTupleDesc(tableId).equals(this.child.getTupleDesc())) {
            throw new DbException("Insert: Child's TupleDesc doesn't match Table's TupleDesc");
        }

        Type[] typeAr = {Type.INT_TYPE};
        String[] fieldAr = {"INSERT_CNT"};
        this.cntTD = new TupleDesc(typeAr, fieldAr);
    }

    public TupleDesc getTupleDesc() {
        return this.cntTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        this.used = false;
        super.open();
    }

    public void close() {
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        this.used = false;
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
        int count = 0;
        while (this.child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, this.child.next());
                ++count;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (!this.used) {
            Tuple tp = new Tuple(this.cntTD);
            tp.setField(0, new IntField(count));
            this.used = true;
            return tp;
        } else {
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    }
}
