package org.example;

import org.apache.curator.framework.CuratorFramework;
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
public class NodeBasicOperateWithCurator {
    @Autowired
    private CuratorFramework client;


    /**
     * 递归创建持久化/临时节点
     * zkserver上查看创建的节点以及数据
     *  [zk: localhost:2181(CONNECTED) 0] ls /
     * [clothes, mq0000000005, pnode, servers, zkblog, zookeeper]
     *
     * [zk: localhost:2181(CONNECTED) 1] get /zkblog/p1
     * Java博客
     */
    @Test
    public void createPersistentNode() throws Exception {
        // 父节点不存在则创建
        String persistentNode = client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/zkblog/p1",
                "Java博客".getBytes(StandardCharsets.UTF_8));
        System.out.println(persistentNode);
    }




    @Test
    public void getNodeData() throws Exception {
        byte[] data = client.getData().forPath("/zkblog/p1");
        System.out.println("p1 = " + new String(data));
    }


    /**
     * 临时节点创建后，客户马上和服务端断开连接，所以临时节点会被删除
     * @throws Exception
     */
    @Test
    public void createEphemeralNode() throws Exception {
        String ephemeralNode = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/zkblog/p2",
                "Zookeeper博客".getBytes(StandardCharsets.UTF_8));
        System.out.println("ephemeralNode = " + ephemeralNode);
        //System.in.read(); //注释发开后当前客户端会阻塞，不会断开和服务端之间得连接，临时节点就不会被删除
    }


    /**
     * 修改节点数据
     * @throws Exception
     */
    @Test
    public void setNodeData() throws Exception {
        byte[] bytes = client.getData().forPath("/zkblog/p1");
        System.out.println("修改前："+new String(bytes));
        client.setData().forPath("/zkblog/p1", "kafka博客".getBytes(StandardCharsets.UTF_8));
        byte[] bytes2 = client.getData().forPath("/zkblog/p1");
        System.out.println("修改后："+new String(bytes2));
        }


    /**
     * 递归删除
     * @throws Exception
     */
    @Test
    public void deleteNode() throws Exception {
        //如果节点下面还有子节点，则不能删除
        //client.delete().forPath("/zkblog");

        client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/zkblog");
    }

}
