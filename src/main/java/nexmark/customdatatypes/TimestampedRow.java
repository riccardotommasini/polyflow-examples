package nexmark.customdatatypes;

import tech.tablesaw.api.Row;

public interface TimestampedRow {

    long getTimestamp();

    Row getRow();

}
