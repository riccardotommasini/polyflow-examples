package nexmark.operators.r2s;

import nexmark.customdatatypes.TestTimestampedRow;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

public class RelationToStreamRow implements RelationToStreamOperator<Table, Row> {
}
