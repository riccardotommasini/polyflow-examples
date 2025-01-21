package nexmark.customdatatypes.tablesaw;

public interface TimestampedElement<I> {

    long getTimestamp();

    I getElement();

}
