package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final DbIterator child;
    private final int aField, gField;
    private final Type aFieldType, gFieldType;
    private final Aggregator.Op op;
    private final TupleDesc mergedTp;
    private DbIterator aggrIter = null;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.aField = afield;
        this.gField = gfield;
        TupleDesc tmpChildTD = this.child.getTupleDesc();
        this.aFieldType = tmpChildTD.getFieldType(this.aField);

        // appLabel is used in the precalc-ed merged Tuple Desc.
        String aggLabel = String.format("%s(%s)", aop.toString(),
                tmpChildTD.getFieldName(this.aField));

        // tpType and tpLabel is used to generate merged tp.
        Type [] tpType;
        String [] tpLabel;

        if (this.gField != Aggregator.NO_GROUPING) {
            this.gFieldType = tmpChildTD.getFieldType(this.gField);
            tpType = new Type[2];
            tpType[0] = this.gFieldType;
            tpType[1] = this.aFieldType;
            tpLabel = new String[2];
            tpLabel[0] = tmpChildTD.getFieldName(this.gField);
            tpLabel[1] = aggLabel;
        } else {
            this.gFieldType = null;
            tpType = new Type[1];
            tpType[0] = this.aFieldType;
            tpLabel = new String[1];
            tpLabel[0] = aggLabel;
        }

        this.op = aop;
        this.mergedTp = new TupleDesc(tpType, tpLabel);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return this.gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        if (this.gField != Aggregator.NO_GROUPING) {
            return this.child.getTupleDesc().getFieldName(this.gField);
        } else {
            return null;
        }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return this.aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        TupleDesc td = this.child.getTupleDesc();
        assert(this.aField >= 0);
        assert(this.aField < td.numFields());
        return td.getFieldName(this.aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return this.op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        this.child.open();
        Aggregator agg;
        if (this.aFieldType == Type.INT_TYPE) {
            agg = new IntegerAggregator(this.gField, this.gFieldType, this.aField, this.op);
        } else {
            agg = new StringAggregator(this.gField, this.gFieldType, this.aField, this.op);
        }
        while (this.child.hasNext()) {
            agg.mergeTupleIntoGroup(this.child.next());
        }
        this.aggrIter = agg.iterator();
        this.aggrIter.open();
	    super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.aggrIter != null && this.aggrIter.hasNext()) {
            return this.aggrIter.next();
        }
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        if (this.aggrIter != null) {
            this.aggrIter.rewind();
        } else {
            this.close();
            this.open();
        }
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
        return this.mergedTp;
    }

    public void close() {
        this.child.close();
        this.aggrIter = null;
        super.close();
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
