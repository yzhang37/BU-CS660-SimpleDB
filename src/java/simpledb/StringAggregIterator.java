package simpledb;

import java.util.*;

public class StringAggregIterator extends Operator {
    private final Type gbType;
    private final HashMap<Object, StringAggregHelper> aggMap;
    private Iterator<Map.Entry<Object, StringAggregHelper>> aggMapIter = null;
    private final TupleDesc resultTD;
    private final Aggregator.Op what;

    public StringAggregIterator(HashMap<Object, StringAggregHelper> aggMap,
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
            Map.Entry<Object, StringAggregHelper> next = this.aggMapIter.next();
            StringAggregHelper helper = next.getValue();
            int retField;
            if (this.what == Aggregator.Op.COUNT) {
                retField = helper.getCount();
            } else {
                throw new DbException("StringAggregIterator: only support COUNT");
            }
            Tuple tuple = new Tuple(this.resultTD);
            if (this.gbType == null)
                tuple.setField(0, new IntField(retField));
            else {
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
