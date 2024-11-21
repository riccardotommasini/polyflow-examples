package nexmark.utils;

public interface Query {

    void execute();
    double getTotalTime();
    double getThroughput();
    double getTimeSpentParsing();
}
