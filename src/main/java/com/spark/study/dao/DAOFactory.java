package com.spark.study.dao;

import com.spark.study.dao.impl.ITaskDAOImpl;
import com.spark.study.domain.Task;

/**
 * 工厂类
 * Created by Administrator on 2018/1/31/031.
 */
public class DAOFactory {
    /**
     * 获取实现类
     * @return
     */
    public static ITaskDAO getTaskDAO(){
        return new ITaskDAOImpl();
    }
}
