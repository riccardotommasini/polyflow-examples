package nexmark.operators.r2r;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.api.Table;

import java.util.List;

public class R2Rq8 implements RelationToRelationOperator<Table> {

    List<String> tvgNames;
    String resName;

    public R2Rq8(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public Table eval(List<Table> list) {
        Table t1, t2;
        if(list.get(0).name().equals("empty") || list.get(1).name().equals("empty"))
            return list.get(0);


        if(list.get(0).name().equals("Person")) {
            t1 = list.get(0);
            t2 = list.get(1);
        }
        else{
            t1 = list.get(1);
            t2 = list.get(0);
        }
        if(t1.isEmpty())
            return t1;
        else if(t2.isEmpty())
            return t2;
        for (int i = 0; i < t2.columnCount(); i++) {
            t2.column(i).setName("Auction." + t2.column(i).name());
        }
        return t1.joinOn("id").inner(t2, "Auction.seller").selectColumns("id", "name");

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
