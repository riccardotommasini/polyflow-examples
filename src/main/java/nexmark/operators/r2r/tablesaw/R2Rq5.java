package nexmark.operators.r2r.tablesaw;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.*;

import static tech.tablesaw.aggregate.AggregateFunctions.count;

public class R2Rq5 implements RelationToRelationOperator<Table> {

    List<String> tvgNames;
    String resName;

    public R2Rq5(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public Table eval(List<Table> list) {
        Table t = list.get(0);
        Map<Long, Long> counter = new HashMap<>();
        if(t.isEmpty())
            return t;


        for(int i =0; i<t.rowCount(); i++){
            Long key = (Long) t.column(0).get(i);
            if(!counter.containsKey(key)){
                counter.put(key, 1L);
            }
            else{
                counter.put(key, counter.get(key)+1);
            }
        }
        long max = 0L;
        max = counter.values().stream().max(Comparator.comparingLong(foo->foo)).get();
        LongColumn col = LongColumn.create("max");
        col.append(max);
        return Table.create(col);


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
