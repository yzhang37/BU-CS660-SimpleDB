package simpledb;

import java.io.Serializable;

/**
 * IndexPredicate compares a field which has index on it against a given value
 * @see simpledb.IndexDbIterator
 */
public class IndexPredicate implements Serializable {
	
    private static final long serialVersionUID = 1L;
	
    /**
     * Constructor.
     *
     * @param fvalue The value that the predicate compares against.
     * @param op The operation to apply (as defined in Predicate.Op); either
     *   Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN, Predicate.Op.EQUAL,
     *   Predicate.Op.GREATER_THAN_OR_EQ, or Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    private final Predicate.Op op;
    private final Field field;
    public IndexPredicate(Predicate.Op op, Field fvalue) {
        this.op = op;
        this.field = fvalue;
    }

    public Field getField() {
        return this.field;
    }

    public Predicate.Op getOp() {
        return this.op;
    }

    /** Return true if the fieldvalue in the supplied predicate
        is satisfied by this predicate's fieldvalue and
        operator.
        @param ipd The field to compare against.
    */
    public boolean equals(IndexPredicate ipd) {
        if (this == ipd) return true;
        else if (ipd != null) {
            return ipd.op.equals(this.op) && ipd.field.equals(this.field);
        }
        return false;
    }

}
