package nexmark.customdatatypes;

import tech.tablesaw.api.Row;

public interface TimestampedElement<I> {

    long getTimestamp();

    I getElement();

}
