package nexmark.content.custom;

import nexmark.customdatatypes.BidEvent;
import org.streamreasoning.polyflow.api.secret.content.Content;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FastMaxContent implements Content<Serializable, Serializable, List<Serializable>> {

    List<Serializable> content = new ArrayList<>();
    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(Serializable serializable) {
        if(content.isEmpty())
            content.add(serializable);
        else {
            BidEvent curr = (BidEvent) content.get(0);
            BidEvent candidate = (BidEvent)serializable;
            if(candidate.price>curr.price) {
                content.set(0, candidate);
            }
        }
    }

    @Override
    public List<Serializable> coalesce() {
        return content;
    }
}
