package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield, afield;
    private final Type gbfieldType;
    private final Op what;

    private final HashMap<Object, StringAggregHelper> aggMap;
    private boolean invalid;
    private StringAggregIterator outputIter;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what)
            throws IllegalArgumentException{
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator: only support COUNT aggregator.");
        } else
            this.what = what;
        this.aggMap = new HashMap<>();
        if (this.gbfield == NO_GROUPING) {
            // only the default value
            this.aggMap.put(0, new StringAggregHelper());
        }
        this.invalidOutput();
    }

    private void invalidOutput() {
        this.invalid = true;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (this.gbfield == NO_GROUPING) {
            this.aggMap.get(0).addKey();
        } else {
            Field field = tup.getField(this.gbfield);
            StringAggregHelper aggHelper;
            if (this.gbfieldType == Type.INT_TYPE) {
                IntField intField = (IntField)field;
                int intKey = intField.getValue();
                if (!this.aggMap.containsKey(intKey)) {
                    this.aggMap.put(intKey, new StringAggregHelper());
                }
                aggHelper = this.aggMap.get(intKey);
            } else {
                StringField strField = (StringField)field;
                String strKey = strField.getValue();
                if (!this.aggMap.containsKey(strKey)) {
                    this.aggMap.put(strKey, new StringAggregHelper());
                }
                aggHelper = this.aggMap.get(strKey);
            }
            aggHelper.addKey();
        }
        this.invalidOutput();
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        if (!this.invalid) {
            return this.outputIter;
        } else {
            this.outputIter = new StringAggregIterator(this.aggMap, this.gbfieldType, this.what);
            this.invalid = false;
            return this.outputIter;
        }
    }

}
