package com.spark.study.domain;

/**
 * Created by Administrator on 2018/3/19/019.
 */
public class SessionRandomExtract {
    private long taskId;
    private String sessionId;
    private String startTime;
    private String searchKeyWord;
    private String clickCategoryids;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getSearchKeyWord() {
        return searchKeyWord;
    }

    public void setSearchKeyWord(String searchKeyWord) {
        this.searchKeyWord = searchKeyWord;
    }

    public String getClickCategoryids() {
        return clickCategoryids;
    }

    public void setClickCategoryids(String clickCategoryids) {
        this.clickCategoryids = clickCategoryids;
    }
}
