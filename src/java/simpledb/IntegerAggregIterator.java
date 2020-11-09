package simpledb;

import java.util.*;

public class IntegerAggregIterator extends Operator {
    private final Type gbType;
    private final HashMap<Object, IntegerAggregHelper> aggMap;
    private Iterator<Map.Entry<Object, IntegerAggregHelper>> aggMapIter = null;
    private final TupleDesc resultTD;
    private final Aggregator.Op what;

    public IntegerAggregIterator(HashMap<Object, IntegerAggregHelper> aggMap,
                                 Type gbType,
                                 Aggregator.Op what) {
        this.aggMap = aggMap;
        this.gbType = gbType;
        this.what = what;
        Type[] tdTypes;
        if (this.gbType == null) {
            tdTypes = new Type[1];
            tdTypes[0] = Type.INT_TYPE;
        } else {
            tdTypes = new Type[2];
            tdTypes[0] = gbType;
            tdTypes[1] = Type.INT_TYPE;
        }
        this.resultTD = new TupleDesc(tdTypes);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.aggMapIter = this.aggMap.entrySet().iterator();
        super.open();
    }

    @Override
    public void close() {
        this.aggMapIter = null;
        super.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (this.aggMapIter == null || !this.aggMapIter.hasNext()) {
            return null;
        } else {
            Map.Entry<Object, IntegerAggregHelper> next = this.aggMapIter.next();
            IntegerAggregHelper helper = next.getValue();
            int retField;
            switch (this.what) {
                case COUNT:
                    retField = helper.getCount(); break;
                case MIN:
                    retField = helper.getMin(); break;
                case MAX:
                    retField = helper.getMax(); break;
                case AVG:
                    if (helper.getCount() == 0) {
                        retField = 0;
                    } else {
                        retField = helper.getSum() / helper.getCount();
                    }
                    break;
                case SUM:
                    retField = helper.getSum(); break;
                default:
                    throw new DbException("IntegerAggregIterator: " + this.what.toString() + " is not implemented yet.");
            }
            Tuple tuple = new Tuple(this.resultTD);
            if (this.gbType == null) {
                // no group by
                tuple.setField(0, new IntField(retField));
            } else {
                if (this.gbType == Type.INT_TYPE)
                    tuple.setField(0, new IntField((Integer)next.getKey()));
                else
                    tuple.setField(0, new StringField((String)next.getKey(), Type.STRING_LEN));
                tuple.setField(1, new IntField(retField));
            }
            return tuple;
        }
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.resultTD;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[0];
    }

    @Override
    public void setChildren(DbIterator[] children) {

    }

}
