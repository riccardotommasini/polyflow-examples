package nexmark.customdatatypes.custom;

public class Q3Event implements Entity{

    public String name;
    public String city;
    public String state;
    public long auction;

    public Q3Event(String name, String city, String state, long auction) {
        this.name = name;
        this.city = city;
        this.state = state;
        this.auction = auction;
    }
    @Override
    public String toString(){
        return "Name: "+ name +", City: "+city+", State: "+state+", Auction: "+auction;
    }
}
