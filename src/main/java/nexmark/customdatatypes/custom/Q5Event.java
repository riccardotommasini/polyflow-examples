package nexmark.customdatatypes.custom;

public class Q5Event implements Entity{


    public long auction;
    public long value;

    public Q5Event(long auction, long value) {
        this.auction = auction;
        this.value = value;
    }
    @Override
    public String toString(){
        return "Auction: "+ auction +", Value: "+value;
    }
}