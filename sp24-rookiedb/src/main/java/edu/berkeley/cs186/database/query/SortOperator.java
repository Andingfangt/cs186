package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double)numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // DONE(proj3_part1): implement
        List<Record> sortedRecord = new ArrayList<>();
        while (records.hasNext()) {
            sortedRecord.add(records.next());
        }
        sortedRecord.sort(this.comparator);
        Run sorted_run = new Run(transaction, this.getSchema());
        sorted_run.addAll(sortedRecord);
        return sorted_run;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // DONE(proj3_part1): implement
        // the return Run
        Run newSortedRun = new Run(transaction, this.getSchema());

        // contains all the run iterator in runs lst.
        List<BacktrackingIterator<Record>> runs_iterator_lst = new ArrayList<>();
        for (Run run : runs) {
            runs_iterator_lst.add(run.iterator());
        }

        PriorityQueue<Pair<Record, Integer>>  pq = new PriorityQueue<>(new RecordPairComparator());
        // initialize the pq with all the smallest record in each Run
        for (int i = 0; i < runs_iterator_lst.size(); i++) {
            BacktrackingIterator<Record> run_iterator = runs_iterator_lst.get(i);
            if (run_iterator.hasNext()) {
                Pair<Record, Integer> new_pair = new Pair<>(run_iterator.next(), i);
                pq.add(new_pair);
            }
        }
         // each time we drop the smallest one, add new record in the same runs if it has next.
        while (!pq.isEmpty()) {
            Pair<Record, Integer> smallest_pair = pq.poll();
            Record smallest_record = smallest_pair.getFirst();
            Integer run_index = smallest_pair.getSecond();
            newSortedRun.add(smallest_record);
            BacktrackingIterator<Record> run_iterator = runs_iterator_lst.get(run_index);
            if (run_iterator.hasNext()) {
                Pair<Record, Integer> new_pair = new Pair<>(run_iterator.next(), run_index);
                pq.add(new_pair);
            }
        }

        return newSortedRun;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // DONE(proj3_part1): implement
        // the return runs
        List<Run> new_runs = new ArrayList<>();
        int N = runs.size();
        int merge_pages = numBuffers - 1;
        int start = 0, end = Integer.min(merge_pages, N);
        // each time we choose B-1 runs, merge them.
        while (start < N) {
            List<Run> current_pass_runs = runs.subList(start, end);
            Run new_run = mergeSortedRuns(current_pass_runs);
            new_runs.add(new_run);
            start = end;
            end = Integer.min(end+merge_pages, N);
        }
        return new_runs;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();

        // DONE(proj3_part1): implement
        // pass0, each time we pass in B pages record, and use sortRun to get a sorted run.
        List<Run> sortedRuns = new ArrayList<>();
        while (sourceIterator.hasNext()) {
            sortedRuns.add(sortRun(getBlockIterator(sourceIterator, this.getSchema(), numBuffers)));
        }

        // pass1->n recursively call mergePass until the newRunsLst is single.
        while (sortedRuns.size() > 1) {
            sortedRuns = mergePass(sortedRuns);
        }

        return sortedRuns.get(0);
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

