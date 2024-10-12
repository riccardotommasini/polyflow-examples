package nexmark.customdatatypes;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.ArrayList;
import java.util.List;

public class PersonRow implements TimestampedRow{

    private Row row;
    private long timestamp;

    public PersonRow(Row row, long timestamp){

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
