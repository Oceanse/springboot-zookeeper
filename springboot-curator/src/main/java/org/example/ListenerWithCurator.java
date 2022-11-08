package org.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.nio.charset.StandardCharsets;


/**
 * zookeeper提供的原生API操作过于烦琐，curator框架是对zookeeper提供的原生API进行了封装，提供了更高级的API接口，使客户端程序员使用zookeeper更加容易及高效。
 * 官网：http://curator.apache.org/
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = BootApplication.class)
public class ListenerWithCurator {
    @Autowired
    private CuratorFramework client;


    /**
     * NodeCache: 对一个指定节点进行监听，监听事件包括指定路径的增删改操作
     * [zk: localhost:2181(CONNECTED) 19] set /clothes pants
     * [zk: localhost:2181(CONNECTED) 20]
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
     * @throws Exception
     */
    @Test
    public void testPathChildrenCache() throws Exception {
        // 参数  客户端，路径 ，缓存数据，是否压缩，线程池
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client,"/clothes",false);
        //绑定监听器
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("子节点变化了");
                System.out.println(pathChildrenCacheEvent);
                //监听子节点的变更,并且拿到变更后的数据
                PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
                //判断类型是否是update
                if(type.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)){
                    System.out.println("=============");
                    //拿到数据
                    byte[] data = pathChildrenCacheEvent.getData().getData();
                    System.out.println("xxxxx"+new String(data,StandardCharsets.UTF_8));
                }
            }
        });
        //开启监听
        pathChildrenCache.start();
        System.in.read();
    }




    /**
     * TreeCache：对整个目录或者所有的节点进行监听，可以设置监听深度
     * <p>
     * [zk: localhost:2181(CONNECTED) 12] create /clothes/pants Ling
     * Created /clothes/pants
     * [zk: localhost:2181(CONNECTED) 13] set /clothes/pants Nike
     * [zk: localhost:2181(CONNECTED) 14]
     * [zk: localhost:2181(CONNECTED) 14] set /clothes/pants playboy
     * [zk: localhost:2181(CONNECTED) 15] delete /clothes/pants
     * <p>
     * 控制台输出：
     * /clothes节点添加/clothes/pants	添加数据为：Ling
     * /clothes/pants节点数据更新	更新数据为：Nike	版本为：1
     * /clothes/pants节点数据更新	更新数据为：playboy	版本为：2
     * /clothes/pants节点被删除
     * <p>
     * 测试发现：只是创建节点，但是不set数据，listener没有输出
     *
     * @throws Exception
     */
    @Test
    public void treeCacheWatch() throws Exception {
        TreeCache treeCache = new TreeCache(client, "/clothes");

        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                ChildData eventData = event.getData();
                switch (event.getType()) {
                    case NODE_ADDED:
                        System.out.println("/clothes节点添加" + eventData.getPath() + "\t添加数据为：" + new String(eventData.getData()));
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
