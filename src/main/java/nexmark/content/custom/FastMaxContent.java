package nexmark.content.custom;

import nexmark.customdatatypes.custom.BidEvent;
import org.streamreasoning.polyflow.api.secret.content.Content;

import nexmark.customdatatypes.custom.Entity;
import java.util.ArrayList;
import java.util.List;

public class FastMaxContent implements Content<Entity, Entity, List<Entity>> {

    List<Entity> content = new ArrayList<>();
    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(Entity Entity) {
        if(content.isEmpty())
            content.add(Entity);
        else {
            BidEvent curr = (BidEvent) content.get(0);
            BidEvent candidate = (BidEvent)Entity;
            if(candidate.price>curr.price) {
                content.set(0, candidate);
            }
        }
    }

    @Override
    public List<Entity> coalesce() {
        return content;
    }
}
