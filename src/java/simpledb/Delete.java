package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private final DbIterator child;
    private final TupleDesc cntTD;
    private boolean used = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        Type[] typeAr = {Type.INT_TYPE};
        String[] fieldAr = {"DELETE_CNT"};
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        int count = 0;
        while (this.child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(tid, this.child.next());
                ++count;
            } catch (IOException ex) {
                ex.printStackTrace();
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
