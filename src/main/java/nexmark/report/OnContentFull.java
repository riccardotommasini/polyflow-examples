package nexmark.report;

import org.streamreasoning.polyflow.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.report.strategies.ReportingStrategy;

/*
    Useful in physical sliding windows when we want to report only when the windows is full
 */
public class OnContentFull implements ReportingStrategy {

    //size for which we want to report
    long size;

    public OnContentFull(long size){
        this.size = size;
    }

    @Override
    public boolean match(Window window, Content content, long l, long l1) {
        return content.size() >= size;
    }
}
