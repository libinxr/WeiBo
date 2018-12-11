package com.atlibin.weibo.controller;

import com.atlibin.weibo.pojo.ContentMessage;
import com.atlibin.weibo.service.WeiboService;
import com.sun.tools.internal.ws.resources.WebserviceapMessages;

import java.io.IOException;
import java.util.List;

/**
 * Created by 11610 on 2018/12/2.
 */
public class WeiboController {
    private static WeiboService weiboService=new WeiboService();

    public static void main(String[] args) throws IOException {
        //初始化连接
        weiboService.InitConnection();
        //初始化命名空间
        //weiboService.initNameSpace();
        //创建表
        //weiboService.createTable();
        //发微博
//        weiboService.publishContent("zhangsan","jintiantianqi haoqinglang");
//        weiboService.publishContent("zhangsan","tiantianxiangshang");
//        weiboService.publishContent("zhangsan","haoahoxuexi");
//
//        weiboService.publishContent("weixiaoxue","weixiaoxue de daigou");

        //weixiaoxue关注zhangsan
        //weiboService.attendUser("zhangsan","weixiaoxue");
        weiboService.cancelAttend("zhangsan","weixiaoxue");
        List<ContentMessage> messages = weiboService.watchWeibo("weixiaoxue");
        for (ContentMessage message : messages) {
            System.out.println(message);
        }
        System.out.println("11111111111");
    }
}
