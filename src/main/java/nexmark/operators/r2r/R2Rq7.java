package nexmark.operators.r2r;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import static tech.tablesaw.aggregate.AggregateFunctions.*;

import java.util.Comparator;
import java.util.List;

public class R2Rq7 implements RelationToRelationOperator<Table> {

    List<String> tvgNames;
    String resName;

    public R2Rq7(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public Table eval(List<Table> list) {
        Table t = list.get(0);
        if(t.isEmpty())
            return t;
        return t.selectColumns("auction", "price");

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
