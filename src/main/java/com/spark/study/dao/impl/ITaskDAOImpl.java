package com.spark.study.dao.impl;

import com.spark.study.dao.ITaskDAO;
import com.spark.study.domain.Task;
import com.spark.study.jdbc.JDBCHelper;

import java.sql.ResultSet;

/**
 * Created by Administrator on 2018/1/31/031.
 */
public class ITaskDAOImpl implements ITaskDAO{


    @Override
    public Task findById(long taskId) {
        final Task task = new Task();
        String sql ="select * from task where task_id=?";
        Object[] params =new Object[]{taskId};
        JDBCHelper helper =JDBCHelper.getInstance();

        helper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()){
                    long taskId =rs.getLong(1);
                    String taskName =rs.getString(2);
                    String createTime =rs.getString(3);
                    String startTime =rs.getString(4);
                    String finishTime =rs.getString(5);
                    String taskType =rs.getString(6);
                    String taskStatus =rs.getString(7);
                    String taskParams =rs.getString(8);
                    task.setTaskId(taskId);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParams);
                }
            }
        });

        return task;
    }
}
