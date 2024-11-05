package nexmark.content;

import nexmark.customdatatypes.TestTimestampedRow;
import nexmark.customdatatypes.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;
import tech.tablesaw.api.Table;

import java.util.function.BiFunction;
import java.util.function.Function;

public class LogicalSlidingContentFactory<I, R> implements ContentFactory<TimestampedElement<I>, TimestampedElement<I>, R> {

    R emptyContent;
    long windowSize;
    Function<TimestampedElement<I>, R> f1;
    BiFunction<R, R, R> sumR;

    public LogicalSlidingContentFactory(R emptyContent, long windowSize, Function<TimestampedElement<I>, R> f1, BiFunction<R, R, R> sumR){
        this.emptyContent = emptyContent;
        this.windowSize = windowSize;
        this.f1 = f1;
        this.sumR = sumR;
    }
    @Override
    public Content<TimestampedElement<I>, TimestampedElement<I>, R> createEmpty() {
        return new EmptyContent<>(emptyContent);
    }

    @Override
    public Content<TimestampedElement<I>, TimestampedElement<I>, R> create() {
        return new LogicalSlidingContent<>(emptyContent, windowSize, f1, sumR);
    }
}
