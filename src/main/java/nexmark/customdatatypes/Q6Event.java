package nexmark.customdatatypes;

import java.io.Serializable;

public class Q6Event implements Serializable {

    public double averagePrice;
    //Auction fields
    public long seller;

    public Q6Event(double averagePrice, long seller) {
        this.averagePrice = averagePrice;
        this.seller = seller;
    }

    @Override
    public String toString(){
        return "Seller: "+seller+", Average Price: "+averagePrice;
    }
}
