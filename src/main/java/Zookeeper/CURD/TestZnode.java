package Zookeeper.CURD;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static java.lang.System.out;

public class TestZnode {
    private static int SESSION_TIMEOUT = 300;
    ZooKeeper zk;
    Watcher wh = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            System.out.println(watchedEvent.toString());
        }
    };

    @Before
    public void setUp() throws IOException {
        zk = new ZooKeeper("node1:2181,node2:2181,node3:2181", TestZnode.SESSION_TIMEOUT, this.wh);
    }

    @After
    public void tearDown() throws InterruptedException {
        zk.close();
    }

    @Test
    public void getData() throws InterruptedException, KeeperException {
        System.out.println(new String(zk.getData("/logZnode",false,null)));
    }
}
