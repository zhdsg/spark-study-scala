package com.spark.study.dao;

import com.spark.study.domain.Task;

/**
 * 任务管理接口
 * Created by Administrator on 2018/1/31/031.
 */
public interface ITaskDAO {

    public Task findById(long taskId);
}
