package nexmark.content.custom;

import nexmark.customdatatypes.custom.AuctionEvent;
import nexmark.customdatatypes.custom.Entity;
import org.streamreasoning.polyflow.api.secret.content.Content;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Q4Content implements Content<Entity, Entity, List<Entity>> {


    List<Entity> validAuctions = new ArrayList<>();
    List<Entity> expiredAuctions = new ArrayList<>();
    List<Entity> reportedAuctions = new ArrayList<>();
    EvictContainerContent bidContent;


    public Q4Content(EvictContainerContent bidContent){
        this.bidContent = bidContent;
    }

    @Override
    public int size() {
        return expiredAuctions.size();
    }

    @Override
    public void add(Entity tableTimestampedElement) {
        //Old expired auctions, we already reported them and can evict them
        reportedAuctions.stream().map(e->(AuctionEvent)e).forEach(e->bidContent.removeKey(e.id));
        reportedAuctions = new ArrayList<>();

        AuctionEvent event = (AuctionEvent) tableTimestampedElement;
        long timestamp = event.timestamp;

        for(int i = validAuctions.size()-1; i>=0; i--){
            AuctionEvent tmp = (AuctionEvent) validAuctions.get(i);
            if(tmp.expires < timestamp){
                expiredAuctions.add(validAuctions.get(i));
                validAuctions.remove(i);
            }
        }
        validAuctions.add(tableTimestampedElement);
    }

    @Override
    public List<Entity> coalesce() {
        expiredAuctions.forEach(a->reportedAuctions.add(a));
        expiredAuctions = new ArrayList<>();
        return reportedAuctions;
    }
}
