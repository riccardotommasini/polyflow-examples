package nexmark.operators.r2r;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.Table;

import java.util.List;

public class R2Rq6 implements RelationToRelationOperator<Table> {

    List<String> tvgNames;
    String resName;

    public R2Rq6(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public Table eval(List<Table> list) {
        Table auction = list.get(0);
        Table bid = list.get(1);
        if(auction.isEmpty())
            return auction;
        else if(bid.isEmpty()){
            return bid;
        }
        for (int i = 0; i < bid.columnCount(); i++) {
            bid.column(i).setName("Bid." + bid.column(i).name());
        }
        return auction.joinOn("id").inner(bid, "Bid.auction")
                .summarize("Bid.price", AggregateFunctions.mean).by("seller");
    }

    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public String getResName() {
        return resName;
    }
}
