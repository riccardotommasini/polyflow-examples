package nexmark.content;

import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;

import java.util.Comparator;
import java.util.function.Function;

public class MaxContentFactory<I, W, R> implements ContentFactory<I, W, R> {

    Function<I, W> f1;
    Function<W, R> f2;
    Comparator<W> compareMax;
    R emptyContent;


    public MaxContentFactory(Function<I, W> f1, Function<W, R> f2, Comparator<W> compareMax, R emptyContent){
        this.f1 = f1;
        this.f2 = f2;
        this.compareMax = compareMax;
        this.emptyContent = emptyContent;
    }

    @Override
    public Content<I, W, R> createEmpty() {
        return new EmptyContent<>(emptyContent);
    }

    @Override
    public Content<I, W, R> create() {
        return new MaxContent<>(f1, f2, compareMax, emptyContent);
    }
}
