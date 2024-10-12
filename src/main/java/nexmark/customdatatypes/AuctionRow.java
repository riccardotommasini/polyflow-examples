package nexmark.customdatatypes;

import tech.tablesaw.api.Row;

public class AuctionRow implements TimestampedRow{

    private Row row;
    private long timestamp;

    public AuctionRow(Row row, long timestamp){
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
