package simpledb;

public class StringAggregHelper extends AggregHelper {
    public void addKey() {
        this.count++;
        this.first = false;
    }
}
