package nexmark.operators.r2r.tablesaw.q3;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.api.Table;

import java.util.List;

public class R2Rq3_join implements RelationToRelationOperator<Table> {

    List<String> tvgNames;
    String resName;

    public R2Rq3_join(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public Table eval(List<Table> list) {
        Table people = list.get(0);
        Table auctions = list.get(1);
        if(people.isEmpty())
            return people;
        else if(auctions.isEmpty())
            return auctions;
        for (int i = 0; i < people.columnCount(); i++) {
            people.column(i).setName("Person." + people.column(i).name());
        }
        return people.joinOn("Person.id").inner(auctions, "seller")
                .selectColumns("Person.name", "Person.city", "Person.state", "id");
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
