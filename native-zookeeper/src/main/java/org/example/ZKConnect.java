package org.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZKConnect {

    static ZooKeeper zk;
    static String zkServer = "localhost:2181";
    static int timeout = 5000;
    static CountDownLatch countDownLatch = new CountDownLatch(1);
    static CountDownLatch countDownLatch2 = new CountDownLatch(1);

    /**
     * 测试连接
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        zk = new ZooKeeper(zkServer, timeout, new Watcher() {
            @Override
            public void process(final WatchedEvent event) {
                Event.EventType type = event.getType();
                Event.KeeperState state = event.getState();
                System.out.println("type = " + type);
                System.out.println("state = " + state);
                while (true) {
                    if (zk.getState() == ZooKeeper.States.CONNECTED) {
                        System.out.println("成功连上服务器");
                        countDownLatch.countDown();
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        });
        System.out.println("连接状态：" + zk.getState());
        countDownLatch.await();
        System.out.println("连接状态2：" + zk.getState());
    }

    @Test
    public void testAsyncConnect() throws InterruptedException, IOException {
        /**
         * 客户端和zk服务端链接是一个异步的过程,构造函数会立刻返回，不会等待与服务器连接成功后返回，实际的连接会在后台进行，也就是构造函数返回后可能连接还没完成；
         * 为了捕获与服务器连接状态的变化，可以传毒一个watcher对象；它将会在连接状态发生变化时候被回调
         * zkServer:连接服务器的ip字符串，可以是一个ip，也可以是多个ip,一个ip代表单机，多个ip代表集群
         * timeout: 连接超时时间
         * watcher: 通知事件，如果有对应的事件触发，则会收到一个通知;如果不需要，那就设置为null
         */
        zk = new ZooKeeper(zkServer, timeout, null);
        System.out.println("连接状态：" + zk.getState());//这里可能还没有连上服务器
        Thread.sleep(5000);
        System.out.println("连接状态2：" + zk.getState());//经过了timeout，应该能连上服务器
    }


    /**
     * 客户端和zk服务端链接是一个异步的过程，构造函数会立刻返回，不会等待与服务器连接成功后返回；
     * 所以如果@Before方法只是 zk = new ZooKeeper(zkServer, timeout， new Watcher() {})，那么@Before方法结束后并不一定保证已成功连上服务器
     *
     * @throws IOException
     */
    @Before
    public void connect() throws IOException, InterruptedException {
        zk = new ZooKeeper(zkServer, timeout, new Watcher() {
            @Override
            public void process(final WatchedEvent event) {
                while (true) {
                    if (zk.getState() == ZooKeeper.States.CONNECTED) {
                        countDownLatch.countDown();
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        });
        System.out.println("连接状态：" + zk.getState());
        countDownLatch.await();
        ;//因为上面连接过程是异步过程，所以需要通过计数锁保证连接成功
        System.out.println("连接状态2：" + zk.getState());
    }


    /**
     * 创建同步临时节点,
     * 同步创建节点的命令发送到zookeeper服务器，节点创建成功之前需要等待阻塞，创建节点成功才会返回，然后继续执行后面的操作；
     * 异步创建节点会在命令发送到Zookeeper服务器之前，就返回继续执行之后的代码，节点创建过程（包括网络通信和服务端的节点创建过程）是异步的
     * 同步调用中，需要处理异常;
     * 异步调用中，方法本身不会抛出异常的，所有的异常都会在回调函数中通过Result Code 来体现。
     *
     * @param path 创建的路径
     * @param data 存储的数据的byte[ ]
     * @param acl  控制权限策略
     *             提供默认的权限OPEN_ACL_UNSAFE、CREATOR_ALL_ACL、READ_ACL_UNSAFE
     *             OPEN_ACL_UNSAFE：完全开放
     *             CREATOR_ALL_ACL：创建该znode的连接拥有所有权限
     *             READ_ACL_UNSAFE：所有的客户端都可读
     */
    public void createSyncTempNode(String path, byte[] data, List<ACL> acl) throws InterruptedException, KeeperException {
        String result = zk.create(path, data, acl, CreateMode.EPHEMERAL);
        System.out.println("创建节点 " + result + " 成功");
    }

    /**
     * zk查询：
     * [zk: localhost:2181(CONNECTED) 71] ls /
     * [clothes, mq0000000005, pnode, servers, testEphemeralNode, zookeeper]
     * [zk: localhost:2181(CONNECTED) 73] get /testEphemeralNode
     * test-data
     *
     * @throws IOException
     */
    @Test
    public void testCreateSyncTempNode() throws IOException, InterruptedException, KeeperException {
        createSyncTempNode("/testEphemeralNodexxxxxxxxxxx", "test-data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);//OPEN_ACL_UNSAFE指定该节点可以被任何人访问
        System.in.read();//阻塞状态下可查看创建的临时节点
    }


    /**
     * 创建异步临时节点
     * 异步调用会在命令发送到Zookeeper服务器之前，就返回继续执行之后的代码，节点创建过程（包括网络通信和服务端的节点创建过程）是异步的
     * <p>
     * 同步调用中，需要处理异常;
     * 异步调用中，方法本身不会抛出异常的，所有的异常都会在回调函数中通过Result Code 来体现。
     *
     * @param path
     * @param data
     * @param acl
     */
    public void createAsyncTempNode(String path, byte[] data, List<ACL> acl) throws InterruptedException, KeeperException {
        AsyncCallback.StringCallback callback = new AsyncCallback.StringCallback() {
            /**
             *
             * @param resultCode 当成功创建节点时，resultCode的值为0； -4表示客户端和服务端连接断开；-110表示指定节点已存在
             * @param path 创建节点的路径
             * @param context 当一个StringCallback类型对象作为多个create方法的参数时，这个参数就很有用了
             * @param NodeName 创建节点的名字，其实与path参数相同。
             */
            @Override
            public void processResult(int resultCode, String path, Object context, String NodeName) {

                if (resultCode == KeeperException.Code.OK.intValue()) {
                    System.out.println("resultCode = " + resultCode);
                    System.out.println("path = " + path);
                    System.out.println("context = " + context);
                    System.out.println("NodeName = " + NodeName);
                    System.out.println("Node created: " + NodeName);
                    countDownLatch2.countDown();
                } else {
                    System.out.println("Failed to create node with error code: " + resultCode);
                }
            }
        };
        String ctx = "{'create':'success'}";
        //一般来说异步调用会在命令发送到Zookeeper服务器之前，就返回继续执行之后的代码。
        zk.create(path, data, acl, CreateMode.EPHEMERAL, callback, ctx);
        countDownLatch2.await();//因为上面节点的创建是异步过程，所以需要通过计数锁保证节点创建成功
        System.out.println("创建成功了");
        //other operation,
    }


    /**
     * zk查询：
     * [zk: localhost:2181(CONNECTED) 77] ls /
     * [clothes, mq0000000005, pnode, servers, testEphemeralNode2, zookeeper]
     * [zk: localhost:2181(CONNECTED) 80] get /testEphemeralNode2
     * test-data2
     *
     * @throws IOException
     */
    @Test
    public void testCreateAsyncTempNode() throws IOException, InterruptedException, KeeperException {
        createAsyncTempNode("/testEphemeralNode2", "test-data2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        System.in.read();//保持阻塞状态来保证临时节点不被删除，方便后面查询
    }


    /**
     * 异步方式获取节点数据
     *
     * @throws IOException
     */
    @Test
    public void testGetNodeData() throws IOException {
        AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int i, String nodePath, Object ctx, byte[] bytes, Stat stat) {
                System.out.println("i = " + i);
                System.out.println("nodePath = " + nodePath);
                System.out.println("nodeData = " + new String(bytes));
                System.out.println("ctx = " + ctx);
                System.out.println("stat = " + stat);

            }
        };
        zk.getData("/testEphemeralNode2", true, dataCallback, "异步获取节点的数据值");
        for (int i = 0; i < 3; i++) {
            System.out.println("节点数据获取是一步过程节点数据获取到之前，也可以继续做其他事情");
        }
        System.in.read();
    }


    /**
     * 修改节点
     * 版本号在 ZooKeeper 中是用于实现乐观锁（Optimistic Locking）机制的一种方式, 用于检测节点是否在更新期间被其他线程修改过，从而避免数据不一致的情况。
     * 版本号是在节点创建时自动分配的，并在每次更新节点数据时递增。
     * 在 setData() 方法中，如果您指定了节点的当前版本号作，那么只有在当前节点的版本号与指定的版本号匹配时，更新操作才会成功。
     * 如果当前节点的版本号与指定的版本号不匹配，更新操作将失败。
     *
     * 注意，当调用 setData() 方法时，版本号需要传递一个整数值，表示期望的节点版本号。如果传递 -1，则表示不检查版本号，直接更新节点数据。
     *
     * @param path
     * @param data
     */
    public void updateAsyncNode(String path, byte[] data) throws InterruptedException, KeeperException {
        final int[] versionBeforeUpdate = new int[1];//获取当前节点的版本号
        AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int i, String nodePath, Object ctx, byte[] bytes, Stat stat) {
                versionBeforeUpdate[0] = stat.getVersion();
                System.out.println("versionBeforeUpdate = " + versionBeforeUpdate[0]);

            }
        };
        zk.getData("/testEphemeralNode2", true, dataCallback, "异步获取节点的数据值");
        Thread.sleep(1000);

        Stat result = zk.setData(path, data, versionBeforeUpdate[0]);
        System.out.println("versionAfterUpdate: " + result.getVersion());//版本号会随着每次更新节点数据时递增
    }


    /**
     * zk查询：
     * [zk: localhost:2181(CONNECTED) 77] ls /
     * [clothes, mq0000000005, pnode, servers, testEphemeralNode2, zookeeper]
     * [zk: localhost:2181(CONNECTED) 80] get /testEphemeralNode2
     * test-data2
     *
     * @throws IOException
     */
    @Test
    public void testUpdateSyncNode() throws IOException, InterruptedException, KeeperException {
        updateAsyncNode("/testEphemeralNode2", "test-data-new".getBytes());
    }


}