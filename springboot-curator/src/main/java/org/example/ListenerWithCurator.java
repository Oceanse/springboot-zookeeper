package org.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


/**
 * zookeeper提供的原生API操作过于烦琐，curator框架是对zookeeper提供的原生API进行了封装，
 * 提供了更高级的API接口，使客户端程序员使用zookeeper更加容易及高效。
 * 官网：http://curator.apache.org/
 */
@SpringBootTest(classes = BootApplication.class)
public class ListenerWithCurator {
    @Autowired
    private CuratorFramework client;


    /**
     * odeCache 提供了一个简单的方式来自动管理节点的缓存，并在节点数据发生变化时通知您的应用程序。
     *
     * nodeChanged方法对一个指定节点进行监听，会在以下情况下被调用：
     * 1 节点数据发生变化：
     * 当节点数据被更新时，nodeChanged 方法会被调用。
     * 您可以通过 client.getData().forPath 方法获取最新的节点数据。
     * 2 节点被删除：
     * 当节点被删除时，nodeChanged 方法同样会被调用。
     * 在这种情况下，您将无法通过 client.getData().forPath 获取节点数据，因为节点已经不存在。
     *
     * [zk: localhost:2181(CONNECTED) 19] set /clothes pants
     * [zk: localhost:2181(CONNECTED) 20] set /clothes shoes
     * [zk: localhost:2181(CONNECTED) 24] delete /clothes
     *
     * 控制台输出：
     * /clothes changed
     * pants
     * /clothes changed
     * shoes
     *  /clothes changed
     * @throws Exception
     */
    @Test
    public void nodeCacheWatch() throws Exception {
        NodeCache nodeCache = new NodeCache(client, "/clothes");

        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("/clothes node changed");
                byte[] bytes = client.getData().forPath("/clothes");
                System.out.println(new String(bytes));
            }
        });
        //开启监听
        nodeCache.start();
        System.in.read();
    }


    /**
     * PathChildrenCache: 对指定路径节点的一级子目录监听，不对该节点的操作监听，对其子目录的增删改操作监听
     * PathChildrenCache 主要用于监听指定路径下的 直接子节点(不能递归监听) 变化，可以分别监听子节点的添加、删除和数据更新。
     *
     * 调用时机:
     * 1 初始化完成 (INITIALIZED)：
     * 当 PathChildrenCache 启动并且已经从 ZooKeeper 获取了所有初始数据时，PathChildrenCacheEvent 的 INITIALIZED 类型事件会被触发。
     *
     * 2 子节点添加 (CHILD_ADDED)：
     * 当一个新的子节点被添加到监听的路径下时，PathChildrenCacheEvent 的 CHILD_ADDED 类型事件会被触发。
     *
     * 3 子节点更新 (CHILD_UPDATED)：
     * 当已存在的子节点的数据被更新时，PathChildrenCacheEvent 的 CHILD_UPDATED 类型事件会被触发。
     *
     * 4 子节点删除 (CHILD_REMOVED)：
     * 当一个已存在的子节点被删除时，PathChildrenCacheEvent 的 CHILD_REMOVED 类型事件会被触发。
     *
     * 监听/sports下的直接子节点的添加、删除和数据更新。
     * [zk: localhost:2181(CONNECTED) 5] create /sports
     * [zk: localhost:2181(CONNECTED) 7] create /sports/football
     * [zk: localhost:2181(CONNECTED) 8] create /sports/basketball
     * [zk: localhost:2181(CONNECTED) 18] delete /sports/football
     * [zk: localhost:2181(CONNECTED) 19] set /sports/basketball nba
     * [zk: localhost:2181(CONNECTED) 20] set /sports/basketball cba
     *
     * 不能监听/sports下的二级子节点
     * [zk: localhost:2181(CONNECTED) 10] create /sports/football/club
     * @throws Exception
     */
    @Test
    public void testpathChildrenCacheWatch() throws Exception {
        PathChildrenCache cache = new PathChildrenCache(client, "/sports", true);

        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)  {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        System.out.println("CHILD_ADDED: " + event.getData().getPath());
                        break;
                    case CHILD_UPDATED:
                        System.out.println("CHILD_UPDATED: " + event.getData().getPath());
                        break;
                    case CHILD_REMOVED:
                        System.out.println("CHILD_REMOVED: " + event.getData().getPath());
                        break;
                    case INITIALIZED:
                        System.out.println("INITIALIZED");
                        break;
                    default:
                        break;
                }

            }
        });

        cache.start(PathChildrenCache.StartMode.NORMAL);
        System.in.read(); // 等待用户输入，以便保持监听运行
    }



    /**
     * TreeCache 主要用于监听整个子树的变化(可以设置监听深度)，包括节点的添加、删除以及数据的更新。它会自动维护一个本地缓存来反映 ZooKeeper 中的数据结构。
     * 调用时机
     * 1 初始化完成 (INITIALIZED)：
     * 当 TreeCache 启动并且已经从 ZooKeeper 获取了所有初始数据时，TreeCacheEvent 的 INITIALIZED 类型事件会被触发。
     * 2 节点添加 (NODE_ADDED)：
     * 当一个新节点被添加到监听的路径下时，TreeCacheEvent 的 NODE_ADDED 类型事件会被触发。
     * 3 节点更新 (NODE_UPDATED)：
     * 当已存在的节点的数据被更新时，TreeCacheEvent 的 NODE_UPDATED 类型事件会被触发。
     * 4 节点删除 (NODE_REMOVED)：
     * 当一个已存在的节点被删除时，TreeCacheEvent 的 NODE_REMOVED 类型事件会被触发。
     *
     * TreeCache和PathChildrenCache监听范围：
     * TreeCache 监听的是整个子树，包括根节点及其所有子节点。
     * PathChildrenCache 只监听指定路径下的直接子节点。
     *
     * [zk: localhost:2181(CONNECTED) 18] create /sports/football/club
     * @throws Exception
     */
    @Test
    public void treeCacheWatch() throws Exception {
        TreeCache treeCache = new TreeCache(client, "/sports");

        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                ChildData eventData = event.getData();
                switch (event.getType()) {
                    case NODE_ADDED:
                        System.out.println("/sports节点添加" + eventData.getPath());
                        break;
                    case NODE_UPDATED:
                        System.out.println(eventData.getPath() + "节点数据更新\t更新数据为：" + new String(eventData.getData()) + "\t版本为：" + eventData.getStat().getVersion());
                        break;
                    case NODE_REMOVED:
                        System.out.println(eventData.getPath() + "节点被删除");
                        break;
                    default:
                        break;
                }
            }
        });
        //开启监听
        treeCache.start();
        System.in.read();
    }


}
