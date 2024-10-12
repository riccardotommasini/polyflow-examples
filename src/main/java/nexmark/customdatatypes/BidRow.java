package nexmark.customdatatypes;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

public class BidRow implements TimestampedRow{

    private Row row;
    private long timestamp;

    public BidRow(Row row, long timestamp){
        this.row = row;
        this.timestamp = timestamp;
    }

    @Override
    public long getTimestamp() {
        return 0;
    }
    @Override
    public Row getRow() {
        return null;
    }
}
