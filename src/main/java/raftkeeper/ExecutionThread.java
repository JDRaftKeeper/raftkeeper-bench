package raftkeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static org.apache.zookeeper.CreateMode.PERSISTENT;

/**
 * Created by JackyWoo on 2021/3/1.
 */
public class ExecutionThread implements Runnable {

    private final int id;
    private ZooKeeper zoo;
    private static final int MAX_RETRIES = 100;

    /// notify main thread that I am done.
    private final CountDownLatch latch;

    /// receive start execution signal
    private final CyclicBarrier barrier;

    private final String workPath;

    public ExecutionThread(CountDownLatch latch, CyclicBarrier barrier, int id) {
        this.latch = latch;
        this.barrier = barrier;
        this.id = id;
        this.workPath = Benchmark.ROOT_PATH + "/" + id;
    }

    public static ZooKeeper createClient() throws IOException, InterruptedException {
        // init zookeeper client
        ZooKeeper zk = new ZooKeeper(Benchmark.nodes, 60000, event -> {
        });

        int retryCount = 0;
        // waiting for connection to be established
        while (zk.getState() != ZooKeeper.States.CONNECTED) {
            Thread.sleep(100);
            if (++retryCount > MAX_RETRIES) {
                System.out.println("Failed to connect server!");
                System.exit(1);
            }
        }

        return zk;
    }

    public void run() {
        try {
            zoo = createClient();

            beforeRun();

            long t2 = System.nanoTime();
            int errorBatchCount = 0;

            long created = 0;
            barrier.await();

            while (System.nanoTime() < t2 + Benchmark.runDuration * 1000_000_000L) {
                // send request by batch size 100 * BATCH_SIZE
                try {
                    int count;
                    if (Benchmark.onlyCreate) {
                        count = sendCreateRequests(created);
                    } else {
                        count = sendMixedRequests();
                    }
                    created += count;
                    Benchmark.totalCnt.addAndGet(count);
                } catch (Exception e) {
                    if (errorBatchCount == 0) {
                        e.printStackTrace();
                    }
                    errorBatchCount++;
                }
            }

            if (errorBatchCount > 0) {
                System.out.println("task " + id + " error bath count is " + errorBatchCount);
            }

            long t3 = System.nanoTime();
            Benchmark.totalTime.addAndGet((t3 - t2) / 1000);
            Benchmark.taskEndTimes[id] = t3 / 1000;

        } catch (Exception e) {
            throw new RuntimeException(e);

        } finally {
            try {
                if (zoo != null) {
                    afterRun();
                    zoo.close();
                }
            } catch (Exception ignored) {
            }
        }

        latch.countDown();
    }

    private String generateKey(long idx) {
        return workPath + "/" + Benchmark.NODE_PREFIX + idx;
    }

    /**
     * create:1%
     * set:8%
     * get:45%
     * list:45%
     * delete:1%
     *
     * @return sent request count
     */
    private int sendMixedRequests() {
        for (long i = 0; i < Benchmark.BATCH_SIZE; i++) {
            String path = generateKey(i);
            try {
                create(path);
                for (int j = 0; j < 8; j++) {
                    setData(path);
                }
                for (int j = 0; j < 45; j++) {
                    getData(path);
                }
                for (int j = 0; j < 45; j++) {
                    getChildren(Benchmark.LIST_REQUEST_PATH);
                }
                delete(path);
            } catch (Exception e) {
                if (i == 0) {
                    e.printStackTrace();
                }
            }
        }
        return 100 * Benchmark.BATCH_SIZE;
    }

    private int sendCreateRequests(long start) throws Exception {
        for (long i = start; i < start + Benchmark.BATCH_SIZE * 100; i++) {
            String path = generateKey(i);
            create(path);
        }
        return Benchmark.BATCH_SIZE * 100;
    }

    private void create(String path) throws Exception {
        long startTime = System.nanoTime();
        try {
            zoo.create(path, Benchmark.payload, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (Exception e) {
            Benchmark.errorCount.incrementAndGet();
            throw e;
        } finally {
            updateStatsRT(startTime);
        }
    }

    private void setData(String path) throws Exception {
        long startTime = System.nanoTime();
        try {
            zoo.setData(path, Benchmark.payload, -1);
        } catch (Exception e) {
            Benchmark.errorCount.incrementAndGet();
            throw e;
        } finally {
            updateStatsRT(startTime);
        }
    }

    private void getData(String path) throws Exception {
        long startTime = System.nanoTime();
        try {
            zoo.getData(path, null, null);
        } catch (Exception e) {
            Benchmark.errorCount.incrementAndGet();
            throw e;
        } finally {
            updateStatsRT(startTime);
        }
    }

    private void getChildren(String path) throws Exception {
        long startTime = System.nanoTime();
        try {
            zoo.getChildren(path, null);
        } catch (Exception e) {
            Benchmark.errorCount.incrementAndGet();
            throw e;
        } finally {
            updateStatsRT(startTime);
        }
    }

    private void updateStatsRT(long startTime) {
        long endTime = System.nanoTime();
        int rt = (int) ((endTime - startTime) / 100_000);
        if (rt < 0 || rt >= Benchmark.RT_CONTAINER_SIZE) {
            rt = Benchmark.RT_CONTAINER_SIZE - 1;
        }
        Benchmark.rtCount[rt].incrementAndGet();
    }

    private void delete(String path) throws Exception {
        long startTime = System.nanoTime();
        try {
            zoo.delete(path, -1);
        } catch (Exception e) {
            Benchmark.errorCount.incrementAndGet();
            throw e;
        } finally {

            updateStatsRT(startTime);
        }
    }

    private void beforeRun() throws Exception {
        try {
            zoo.create(workPath, Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException.NodeExistsException nodeExistsException) {
            // delete child nodes if exist
            deleteRecursively(zoo, workPath);
            // recreate it to reset cversion
            try {
                zoo.create(workPath, Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
            } catch (KeeperException ignored) {
            }
        }
    }

    private void afterRun() throws Exception {
        deleteRecursively(zoo, workPath);
    }

    static void createRootAndNodesForListRequests() throws Exception {
        ZooKeeper zoo = createClient();
        try {
            zoo.create(Benchmark.ROOT_PATH, Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException.NodeExistsException ignored) {
        }
        try {
            zoo.create(Benchmark.LIST_REQUEST_PATH, Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException.NodeExistsException ignored) {
        }

        for (int i = 0; i < Benchmark.LIST_PATH_CHILD_COUNT; i++) {
            try {
                zoo.create(Benchmark.LIST_REQUEST_PATH + "/" + Benchmark.generateRandomString(Benchmark.LIST_PATH_CHILD_SIZE), Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) {
            }
        }
        zoo.close();
    }

    public static void deleteRootAndNodesForListRequests() throws Exception {
        ZooKeeper zoo = createClient();
        deleteRecursively(zoo, Benchmark.LIST_REQUEST_PATH);
        deleteRecursively(zoo, Benchmark.ROOT_PATH);
        zoo.close();
    }

    public static void deleteRecursively(ZooKeeper zoo, String path) throws KeeperException, InterruptedException {
        try {
            List<String> children = zoo.getChildren(path, false);
            for (String child : children) {
                String childPath = path + "/" + child;
                deleteRecursively(zoo, childPath);
            }
            zoo.delete(path, -1);
        } catch (KeeperException.NoNodeException ignored) {
        }
    }

}
