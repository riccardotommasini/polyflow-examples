package nexmark.content.custom;

import org.streamreasoning.polyflow.api.secret.content.Content;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FastAccumulatorContent implements Content<Serializable, Serializable, List<Serializable>> {
    List<Serializable> content = new ArrayList<>();

    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(Serializable serializable) {
        content.add(serializable);
    }

    @Override
    public List<Serializable> coalesce() {
        return content;
    }
}
