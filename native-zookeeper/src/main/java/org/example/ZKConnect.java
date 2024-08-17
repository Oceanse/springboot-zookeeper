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
     * 客户端和zk服务端链接是一个异步的过程，构造函数会立刻返回，不会阻塞当前线程，而是继续执行后续代码,实际的连接会在后台进行
     * 由于提供了 null 作为 Watcher，因此不会有任何 Watcher 回调。
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void testAsyncConnect() throws InterruptedException, IOException {

        zk = new ZooKeeper(zkServer, timeout, null);
        System.out.println("连接状态：" + zk.getState());//这里可能还没有连上服务器
        Thread.sleep(5000);
        System.out.println("连接状态2：" + zk.getState());//经过了timeout，应该能连上服务器
    }


    /**
     * 如果没有为特定的 ZNode 设置监视器，那么 Watcher 的 process 方法只会因为会话状态的变化而被调用
     * 这包括以下几种情况：
     * 会话建立：当 ZooKeeper 客户端成功连接到 ZooKeeper 服务器并建立会话时，process 方法会被调用。
     * 会话状态变化：当会话的状态发生变化时（例如从 Connecting 变为 SyncConnected)，process 方法会被调用。
     *
     * @throws IOException
     */
    @Test
    public void connect() throws IOException, InterruptedException {
        zk = new ZooKeeper(zkServer, timeout, new Watcher() {
            @Override
            public void process(final WatchedEvent event) {
                Event.KeeperState state = event.getState();
                while (true) {
                    System.out.println("state = " + state);//当会话的状态发生变化时（例如从 Connecting 变为 SyncConnected)，process 方法会被调用
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
    public void createSyncTempNode(String path, byte[] data, List<ACL> acl) throws InterruptedException, KeeperException, IOException {
        zk = new ZooKeeper(zkServer, timeout, null);
        Thread.sleep(5000);

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
     *
     *  processResult 方法将在以下情况下被调用：
     * 1 异步创建节点成功：
     * 当节点成功创建时，processResult 方法会被调用，其中 resultCode 的值将是 KeeperException.Code.OK.intValue()，即 0。
     * 2 异步创建节点失败：
     * 如果创建节点操作失败，例如因为节点已经存在或客户端与服务器之间的连接断开等原因，processResult 方法同样会被调用，此时 resultCode 将包含一个错误码。
     *
     * @param path
     * @param data
     * @param acl
     */
    public void createAsyncTempNode(String path, byte[] data, List<ACL> acl) throws InterruptedException, KeeperException, IOException {
        zk = new ZooKeeper(zkServer, timeout, null);
        Thread.sleep(5000);

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
     * processResult 方法的调用时机
     * 1 成功获取节点数据：
     * 当节点数据成功获取时，processResult 方法被调用，其中
     * i 的值为 KeeperException.Code.OK.intValue()，即 0。
     * nodePath 参数是获取数据的节点路径。
     * bytes 参数是节点的数据。
     * ctx 参数是您传递给 zk.getData 方法的上下文对象。
     * stat 参数是节点的状态信息。
     *
     * 2获取节点数据失败：
     * 如果获取节点数据失败，processResult 方法同样被调用，其中 i 将包含一个错误码。
     *
     * @throws IOException
     */
    @Test
    public void testGetNodeData() throws IOException, InterruptedException {
        zk = new ZooKeeper(zkServer, timeout, null);
        Thread.sleep(5000);

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
        zk.getData("/testEphemeralNode2", false, dataCallback, "异步获取节点的数据值");
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