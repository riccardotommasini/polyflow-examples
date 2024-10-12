package nexmark.customdatatypes;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.Iterator;

public class TestTimestampedRow{

    private Table row;
    private long timestamp;

    public TestTimestampedRow(Table row, long timestamp){
        this.row = row;
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return this.timestamp;
    }
    public Table getRow() {
        return this.row;
    }

}
