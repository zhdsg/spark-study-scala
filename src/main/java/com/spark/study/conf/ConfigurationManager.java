package com.spark.study.conf;

/**
 * Created by Administrator on 2018/1/24/024.
 */


import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 */
public  class ConfigurationManager {

    private static Properties prop =new Properties();

    /**
     * 静态代码块
     */
    static{
        try{
            InputStream in =ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            prop.load(in);
        }catch(Exception e){
            e.printStackTrace();
        }

    }
    /**
     * 获取key对应的value
     * @param key
     * @return
     */
    public static String getProperty(String key){
        return prop.getProperty(key);
    }

    /**
     * 从配置文件中获取int值的value
     * @param key
     * @return
     */
    public static Integer getInteger(String key){
        String value =getProperty(key);
        try{
            return Integer.valueOf(value);
        }catch(Exception e){
            e.printStackTrace();
        }
        return 0;
    }
    public static boolean getBoolean(String key){
        String value =getProperty(key);
        try{
            return Boolean.valueOf(value);
        }catch(Exception e){
            e.printStackTrace();
        }
        return false;
    }
}
