package raftkeeper;

import org.apache.commons.cli.*;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A benchmark tool to test RaftKeeper or ZooKeeper.
 *
 * Created by JackyWoo on 2021/3/1.
 */
public class Benchmark {
    public static final byte[] BLANK_BYTE = "".getBytes();
    public static final int BATCH_SIZE = 10;
    public static final String ROOT_PATH = "/raftkeeper-bench";
    public static final String LIST_REQUEST_PATH = ROOT_PATH + "/list_request_path";
    public static final int LIST_PATH_CHILD_COUNT = 100;
    public static final int LIST_PATH_CHILD_SIZE = 50;
    public static String NODE_PREFIX;

    static {
        byte[] bytes = new byte[2];
        for (int i = 0; i < 2; i++) {
            bytes[i] = '0';
        }
        NODE_PREFIX = new String(bytes);
    }

    // connection string
    public static String nodes;
    public static int parallel;
    public static int payloadSize;
    // in second
    public static int runDuration;
    public static boolean onlyCreate = true;

    // znode value used in create and set request
    public static byte[] payload;

    // total request count in this benchmark
    public static final AtomicLong totalCnt = new AtomicLong(0);

    // Real execution time which does not contain  the time of before run and after run.
    // If the parallel is 5 and every task run 10 second the value is 5 * 10 = 50 second.
    // It is measure in microsecond
    public static final AtomicLong totalTime = new AtomicLong(0);

    // Real task execution wall time which does not contain the time of before run and after run.
    // We set up a cyclic barrier to signal all tasks to start running and the time is t1.
    // When task finishes we record all the end timestamp, and we take the max one called t2.
    // wallTime = t2 - t1
    public static long wallTime = 0;

    // task start time microsecond
    public static long taskStartTime;

    /// max value in taskEndTimes, measured in microsecond
    public static long maxTaskEndTime;

    // task end time of every thread, we use the max value to calculate tps
    public static long[] taskEndTimes;

    // for tp50 tp90 tp99 tp999
    public static final int RT_CONTAINER_SIZE = 10000;

    // 100 * microsecond
    public static final AtomicLong[] rtCount = new AtomicLong[RT_CONTAINER_SIZE];

    // benchmark indexes
    private long tps;
    // microsecond
    private long avgRT = 0L;
    private long tp30 = 0L;
    private long tp50 = 0L;
    private long tp90 = 0L;
    private long tp99 = 0L;
    private long tp999 = 0L;

    public static final AtomicLong errorCount = new AtomicLong(0);


    public static void main(String[] args) throws Exception {
        Benchmark main = new Benchmark();
        main.benchmark(args);
    }

    public void benchmark(String[] args) throws Exception {
        parseConfig(args);
        runBenchmark();
        calculateStatistics();
        reportResult();
        System.exit(0);
    }

    public void parseConfig(String[] args) {

        Options options = getOptions();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help")) {
                formatter.printHelp("raftkeeper-bench", options);
                System.exit(0);
            }

            if (!cmd.hasOption("nodes")) {
                System.out.println("You must specify nodes!");
                System.exit(1);
            }

            nodes = cmd.getOptionValue("nodes");
            parallel = Integer.parseInt(cmd.getOptionValue("parallel", "10"));
            payloadSize = Integer.parseInt(cmd.getOptionValue("payload_size", "100"));
            runDuration = Integer.parseInt(cmd.getOptionValue("run_duration", "60"));
            onlyCreate = cmd.getOptionValue("mode", "mix").equals("create");
            taskEndTimes = new long[parallel];

            System.out.println("\nBenchmark configuration:");
            System.out.print("nodes:" + nodes);
            System.out.print(" parallel:" + parallel);
            System.out.print(", payload_size:" + payloadSize);
            System.out.print(", run_duration:" + runDuration + "s");
            System.out.print(", mode:" + (onlyCreate ? "create-100%" : "create-1% set-8% get-45% list-45% delete-1%"));
            System.out.println();

        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("raftkeeper-bench", options);

            System.exit(1);
        }
    }

    private static Options getOptions() {
        Options options = new Options();

        options.addOption("c", "nodes", true, "RaftKeeper or ZooKeeper connection string, such as 'xx1:8101,xx2:8101,xx3:8101'.");
        options.addOption("p", "parallel", true, "Thread size running the benchmark.");
        options.addOption("s", "payload_size", true, "Value size of every node when sending create and set requests.");
        options.addOption("t", "run_duration", true, "Approximate run duration in seconds.");
        options.addOption("m", "mode", true, "Test execution mode: 'create' or 'mix'. " +
                "If create, only send create requests to backend, every created node value is 100 bytes. " +
                "If mix, create-1% set-8% get-45% list-45% delete-1%. Request details: 1.List: 100 children every child 50 bytes; 2.Get: node value is 100 bytes; 3.Create: every node value is 100 bytes; 4.Set: every node value is 100 bytes; 5.Delete: delete the created nodes.");
        options.addOption("h", "help", false, "Print help.");
        return options;
    }

    private void calculateStatistics() {

        for (int i = 0; i < parallel; i++) {
            if (maxTaskEndTime < taskEndTimes[i])
                maxTaskEndTime = taskEndTimes[i];
        }

        wallTime = maxTaskEndTime - taskStartTime;

        tps = (totalCnt.get() / ((maxTaskEndTime - taskStartTime) / 1000_000));
        avgRT = totalTime.get() / totalCnt.get();

        long summ = 0;
        for (int i = 0; i < RT_CONTAINER_SIZE; i++) {
            summ += rtCount[i].get();
        }

        long sum = 0L;
        double tp;

        for (int i = 0; i < RT_CONTAINER_SIZE; i++) {
            sum += rtCount[i].get();
            tp = ((double) sum) / summ;
            if (tp30 == 0 && tp >= 0.30D) {
                tp30 = i * 100;
            }
            if (tp50 == 0 && tp >= 0.50D) {
                tp50 = i * 100;
            }
            if (tp90 == 0 && tp >= 0.90D) {
                tp90 = i * 100;
            }
            if (tp99 == 0 && tp >= 0.99D) {
                tp99 = i * 100;
            }
            if (tp >= 0.999D) {
                tp999 = i * 100;
                break;
            }
        }
    }

    private void reportResult() {
        System.out.println("\nBenchmark result(time measured in microsecond):");
        System.out.println("parallel,tps,avgRT(us),TP90(us),TP99(us),TP999(us),wall_time(us),total_time(us),total_request,fail_request");
        System.out.println(parallel + "," + tps + "," + avgRT + "," + tp90 + "," + tp99 + "," + tp999 + "," + wallTime + "," + totalTime + "," + totalCnt.get() + "," + errorCount.get());
    }

    private void runBenchmark() throws Exception {
        // init rt container
        for (int i = 0; i < RT_CONTAINER_SIZE; i++) {
            rtCount[i] = new AtomicLong(0);
        }

        // generate payload
        Benchmark.generatePayload();

        ExecutionThread.createRootAndNodesForListRequests();

        // run benchmark
        CountDownLatch endLatch = new CountDownLatch(parallel);
        CyclicBarrier barrier = new CyclicBarrier(parallel, () -> taskStartTime = System.nanoTime() / 1000);

        for (int i = 0; i < parallel; i++) {
            Thread thread = new Thread(new ExecutionThread(endLatch, barrier, i), "worker-" + i);
            thread.start();
        }

        endLatch.await();
        ExecutionThread.deleteRootAndNodesForListRequests();
    }

    public static void generatePayload() {
        payload = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            payload[i] = (byte) 0;
        }
    }

    public static String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();

        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(chars.length());
            sb.append(chars.charAt(index));
        }
        return sb.toString();
    }
}
