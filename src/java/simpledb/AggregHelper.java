package simpledb;

import java.io.Serializable;

abstract public class AggregHelper implements Serializable {
    protected int count = 0;
    transient protected boolean first = true;

    public int getCount() {
        return first ? 0: this.count;
    }

    public void reset() {
        this.count = 0;
        this.first = true;
    }
}
