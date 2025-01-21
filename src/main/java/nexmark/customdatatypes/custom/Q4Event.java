package nexmark.customdatatypes.custom;

public class Q4Event implements Entity {

    public double averagePrice;
    //Auction fields
    public long category;

    public Q4Event(double averagePrice, long category) {
        this.averagePrice = averagePrice;
        this.category = category;
    }

    @Override
    public String toString(){
        return "Category: "+category+", Average Price: "+averagePrice;
    }
}
