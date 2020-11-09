package simpledb;

public class IntegerAggregHelper extends AggregHelper {
    protected int sum = 0, min = 0, max = 0;

    public Integer getSum() {
        return this.first ? null: this.sum;
    }
    public Integer getMin() {
        return this.first ? null : this.min;
    }

    public Integer getMax() {
        return this.first ? null: this.max;
    }

    @Override
    public void reset() {
        this.sum = 0;
        this.min = 0;
        this.max = 0;
        super.reset();
    }

    public void addKey(int key) {
        if (this.first) {
            this.sum = this.min = this.max = key;
            this.first = false;
        } else {
            this.min = Math.min(key, this.min);
            this.max = Math.max(key, this.max);
            this.sum += key;
        }
        this.count++;
    }
}
