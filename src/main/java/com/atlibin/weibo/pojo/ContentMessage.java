package com.atlibin.weibo.pojo;


import java.sql.Date;
import java.text.SimpleDateFormat;

/**
 * Created by 11610 on 2018/12/2.
 */
public class ContentMessage {
    private String userId;
    private long timeStamp;
    private String content;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        Date date = new Date(this.timeStamp);
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return "ContentMessage{" +
                "userId='" + userId + '\'' +
                ", timeStamp='" + sdf.format(date) + '\'' +"\t"+
                ", content='" + content + '\'' +
                '}';
    }
}
