package com.atlibin.weibo.service;

import com.atlibin.weibo.dao.HbaseDao;
import com.atlibin.weibo.dao.InboxDao;
import com.atlibin.weibo.dao.RelationDao;
import com.atlibin.weibo.dao.WeiboDao;
import com.atlibin.weibo.pojo.ContentMessage;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Time;
import java.util.Iterator;
import java.util.List;

/**
 * Created by 11610 on 2018/12/2.
 */
public class WeiboService {

    ThreadLocal<Connection> threadLocal=new ThreadLocal<Connection>();
    private HbaseDao hbaseDao=new HbaseDao();
    private WeiboDao weiboDao=new WeiboDao();
    private InboxDao inboxDao=new InboxDao();
    private RelationDao relationDao=new RelationDao();

    public synchronized void InitConnection() throws IOException {
        Connection connection = threadLocal.get();
        if(connection==null){
            connection= hbaseDao.createConnection();
           threadLocal.set(connection);
        }
    }


    /**
     * 初始化命名空间
     */
    public void initNameSpace() throws IOException {
        hbaseDao.createNameSpace(threadLocal.get());
    }

    /**
     * 创建表
     */
    public void createTable() throws IOException {
        Connection connection = threadLocal.get();
        weiboDao.createTable(connection);
        relationDao.createTable(connection);
        inboxDao.createTable(connection);
    }


    /**
     *
     * @param userId 用户id
     * @param content 微博内容
     */
    public void publishContent(String userId,String content) throws IOException {
        Connection connection = threadLocal.get();
        //将微博添加到微博内容表
       String rowKey= weiboDao.insertData(connection,userId,content);
        String[] strings = rowKey.split("_");
        String timeStamp=strings[1];
        //获取本用户的fans的userId
        List<String> fans=relationDao.getFans(connection,userId);
        //将微博内容刷新到所有粉丝的微博收件箱
        if(fans.size()>0)
            inboxDao.insertDatas(connection,fans,userId,timeStamp,rowKey);
    }


    /**
     * 关注新用户
     * @param attendId
     * @param userId
     */
    public void attendUser(String attendId,String userId) throws IOException {
        Connection connection = threadLocal.get();
        //粉丝多了一个关注的人
        //被关注者多了一个粉丝
        relationDao.insertAttendUser(connection,attendId,userId);
        //获取被关注者五条微博添加到粉丝收件箱
        List<byte[]> weibos= weiboDao.getWeibos(connection,attendId);
       //添加到粉丝的收件箱
        inboxDao.insertDatas(connection,userId,attendId,weibos);
       /* String timeStamp=null;
        Iterator<byte[]> iterator = weibos.iterator();
        while (iterator.hasNext()){
            String rowKey = Bytes.toString(iterator.next());
            String[] strings = rowKey.split("_");
            timeStamp=strings[1];
            inboxDao.insertDatas(connection,userId,attendId,timeStamp,rowKey);
        }*/
    }


    /**
     * 取消关注
     * @param attendId
     * @param userId
     * @throws IOException
     */
    public void cancelAttend(String attendId,String userId) throws IOException {
        Connection connection = threadLocal.get();
        //关系表中删除关注人，被关注人删除粉丝
        relationDao.deleteAttendUser(connection,attendId,userId);
        //收件箱表中删除消息
        inboxDao.deleteAttendMessage(connection,attendId,userId);
    }


    /**
     * 查看微博
     * @param connection
     * @param userId
     * @return
     * @throws IOException
     */
    public List<ContentMessage> watchWeibo(String userId) throws IOException {
        Connection connection = threadLocal.get();
        //获得用户的收件箱中5条数据的4条
        List<byte[]> rowKeyList =inboxDao.getMessages(connection,userId);
        //根据得到的rowKey取出微博内容
        List<ContentMessage> contentList=weiboDao.getContent(connection,rowKeyList);
        return contentList;
    }
}
