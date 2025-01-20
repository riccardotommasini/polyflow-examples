package nexmark.content.custom;

import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FastAccumulatorFactory implements ContentFactory<Serializable, Serializable, List<Serializable>> {
    @Override
    public Content<Serializable, Serializable, List<Serializable>> createEmpty() {
        return new EmptyContent<>(new ArrayList<>());
    }

    @Override
    public Content<Serializable, Serializable, List<Serializable>> create() {
        return new FastAccumulatorContent();
    }
}
