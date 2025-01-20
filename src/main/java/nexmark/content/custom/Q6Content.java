package nexmark.content.custom;

import nexmark.customdatatypes.AuctionEvent;
import nexmark.customdatatypes.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import tech.tablesaw.api.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Q6Content implements Content<Serializable, Serializable, List<Serializable>> {


    List<Serializable> validAuctions = new ArrayList<>();
    Queue<Serializable> expiredAuctions = new LinkedList<>();
    int windowSize;
    EvictContainerContent bidContent;


    public Q6Content(int windowSize, EvictContainerContent bidContent){
        this.windowSize = windowSize;
        this.bidContent = bidContent;
    }

    @Override
    public int size() {
        return expiredAuctions.size();
    }

    @Override
    public void add(Serializable tableTimestampedElement) {
        AuctionEvent event = (AuctionEvent) tableTimestampedElement;
        long timestamp = event.timestamp;
        for(int i = validAuctions.size()-1; i>=0; i--){
            AuctionEvent tmp = (AuctionEvent) validAuctions.get(i);
            if(tmp.expires < timestamp){
                expiredAuctions.offer(validAuctions.get(i));
                validAuctions.remove(i);
                while(!expiredAuctions.isEmpty() && expiredAuctions.size() > windowSize) {
                    AuctionEvent auction = (AuctionEvent) expiredAuctions.poll();
                    bidContent.removeKey(auction.id);
                }
            }
        }
        validAuctions.add(tableTimestampedElement);
    }

    @Override
    public List<Serializable> coalesce() {
        return new ArrayList<>(expiredAuctions);
    }
}
