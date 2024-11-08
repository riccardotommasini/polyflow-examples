package nexmark.operators.r2s;

import nexmark.customdatatypes.TestTimestampedRow;
import org.checkerframework.checker.units.qual.A;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RelationToStreamRow implements RelationToStreamOperator<Table, Row> {

     @Override
     public Stream<Row> eval(Table sml, long ts) {
         List<Table> list = new ArrayList<>();
         for(int i =0; i<sml.rowCount(); i++){
             Table tmp = sml.emptyCopy();
             tmp.append(sml.row(i));
             list.add(tmp);
         }

         return list.stream().map(t->t.row(0));
    }
}
