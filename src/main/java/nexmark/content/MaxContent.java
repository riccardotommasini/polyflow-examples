package nexmark.content;

import org.streamreasoning.polyflow.api.secret.content.Content;

import java.util.Comparator;
import java.util.function.Function;

public class MaxContent<I, W, R> implements Content<I, W, R> {

    W content;
    Function<I, W> f1;
    Function<W, R> f2;
    Comparator<W> compareMax;
    R emptyContent;

    public MaxContent(Function<I, W> f1, Function<W, R> f2, Comparator<W> compareMax, R emptyContent){
        this.f1 = f1;
        this.f2 = f2;
        this.compareMax = compareMax;
        this.emptyContent = emptyContent;
    }
    @Override
    public int size() {
        return content == null?0:1;
    }

    @Override
    public void add(I i) {
        W element = f1.apply(i);
        if(compareMax.compare(content, element) < 0){ //content is smaller than element
            content = element;
        }
    }

    @Override
    public R coalesce() {
        return f2.apply(content);
    }
}
