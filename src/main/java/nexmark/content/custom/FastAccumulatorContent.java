package nexmark.content.custom;

import org.streamreasoning.polyflow.api.secret.content.Content;

import nexmark.customdatatypes.custom.Entity;
import java.util.ArrayList;
import java.util.List;

public class FastAccumulatorContent implements Content<Entity, Entity, List<Entity>> {
    List<Entity> content = new ArrayList<>();

    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(Entity Entity) {
        content.add(Entity);
    }

    @Override
    public List<Entity> coalesce() {
        return content;
    }
}
