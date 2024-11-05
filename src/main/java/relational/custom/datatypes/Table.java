package relational.custom.datatypes;

import java.util.Iterator;
import java.util.List;

public interface Table {

    Table join(Table left);

    Table filter();
    Table project();
    Table groupBy();
    Iterator<Row> iterator();
    List<String> getColumnsNames();
    List<String> getColumnsTypes();
    List<Row> values();
    Schema getSchema();
    

}
