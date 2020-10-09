package com.hy.flinktest.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Peng Xiaodong
 * @date 2019-5-16 15:24:05
 */
@Slf4j
public class ZookeeperService implements Watcher {

    private ZooKeeper zk;

    public static final String etlAddTaskPath = "/etlAddTaskPath"; //zk节点必须"/"开头

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public ZookeeperService()
            throws IOException, InterruptedException {
        String zkConnectStr = "localhost:2181";//集群逗号分开
        zk = new ZooKeeper(zkConnectStr, 1800000, this);
        countDownLatch.await();
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            countDownLatch.countDown();
        }
    }

    public void saveOffset(String path, long offset)
            throws KeeperException, InterruptedException {
        log.info("保存offset到zk, path = {}, offset = {}", path, offset);
        byte[] bytes = String.valueOf(offset).getBytes();
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            zk.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        zk.setData(path, bytes, -1);
    }

    public long getOffset(String path)
            throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            log.info("zk不存在该路径，offset设置为0. zkPath = {}", path);
            return 0;
        }
        byte[] data = zk.getData(path, false, stat);
        if (data == null || data.length == 0) {
            return 0;
        }
        String dataStr = new String(data);
        if (StringUtils.isNumeric(dataStr)) {
            return Long.valueOf(dataStr);
        } else {
            return 0;
        }
    }

    /**
     * 是否存在相同的抽取任务，如果存在则直接停止，否则继续运行。
     * 通过在zk上注册临时节点的方式来判定。
     */
    public boolean existSameTask(String topicName, int partitionIndex)
            throws KeeperException, InterruptedException {
        String zkTaskPath = etlAddTaskPath + "/" + topicName + "/" + partitionIndex;

        //如果父节点不存在则创建父节点
        String parentPath = etlAddTaskPath;
        if (!existNode(parentPath)) {
            zk.create(parentPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        String subPath = etlAddTaskPath + "/" + topicName;
        if (!existNode(subPath)) {
            zk.create(subPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        if (existNode(zkTaskPath)) {
            log.error("zk存在相同的任务，请先关闭后再执行！zkTaskPath={}", zkTaskPath);
            return true;
        } else {
            String path = zk.create(zkTaskPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            log.info("创建临时zk节点：{}", path);
            return false;
        }
    }

    public boolean existNode(String path)
            throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        return stat != null;
    }

    public void createNodeIfNotExist(String path, CreateMode createMode)
            throws KeeperException, InterruptedException {
        if (!existNode(path)) {
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        }
    }

    /**
     * 如果保存offset的节点不存在，则创建。
     * @param topicName 主题名称
     * @param partitionIndex 分区号
     */
    public void createOffsetNodeIfNotExist(String topicName, int partitionIndex)
            throws KeeperException, InterruptedException {
        createNodeIfNotExist(etlAddTaskPath, CreateMode.PERSISTENT);
        createNodeIfNotExist(
                etlAddTaskPath + "/" + topicName,
                CreateMode.PERSISTENT);
        createNodeIfNotExist(
                etlAddTaskPath + "/" + topicName + "/" + partitionIndex,
                CreateMode.PERSISTENT);
    }

}
