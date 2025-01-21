package nexmark.customdatatypes.tablesaw;

import tech.tablesaw.api.Table;

public class TestTimestampedRow implements TimestampedElement<Table>{

    private Table row;
    private long timestamp;

    public TestTimestampedRow(Table row, long timestamp){
        this.row = row;
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return this.timestamp;
    }
    public Table getElement() {
        return this.row;
    }

}
