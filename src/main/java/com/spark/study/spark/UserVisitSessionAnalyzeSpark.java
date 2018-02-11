package com.spark.study.spark;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.spark.study.conf.ConfigurationManager;
import com.spark.study.constrants.Constants;
import com.spark.study.dao.DAOFactory;
import com.spark.study.dao.ITaskDAO;
import com.spark.study.domain.Task;
import com.spark.study.utils.MockData;
import com.spark.study.utils.ParamUtils;
import com.spark.study.utils.StringUtils;
import com.spark.study.utils.ValidUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import org.apache.spark.sql.hive.HiveContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Iterator;

/**
 * 用户访问session分析作业
 * Created by Administrator on 2018/1/31/031.
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] arg0) {
        arg0=new String[]{"1"};
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟数据
        mockData(sc, sqlContext);
        //创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        //获取任务参数和实例
        long taskId = ParamUtils.getTaskIdFromArgs(arg0);
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSON.parseObject(task.getTaskParam());
        //进行session粒度的数据聚合
        JavaRDD<Row> actionRDD = getActionRDDByDataRange(sqlContext,taskParam);
        JavaPairRDD<String,String> sessionid2AggrInfoRDD =aggregateBySession(sqlContext, actionRDD);

        System.out.println(sessionid2AggrInfoRDD.count());


        //针对session粒度的聚合数据，进行筛选参数进行指定的筛选
        JavaPairRDD<String ,String > filterSessionId2AggrInfoRDD =filterSession(sessionid2AggrInfoRDD,taskParam);

        for(Tuple2<String,String> tuple : sessionid2AggrInfoRDD.take(1)){
            System.out.println(tuple._2());
        }
        System.out.println(filterSessionId2AggrInfoRDD.count());
        //关闭上下文
        sc.close();

    }

    /**
     * 获取sqlContext
     *
     * @param sc
     * @return
     */
    public static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }

    }

    /**
     * 生成模拟数据
     *
     * @param sc
     * @param sqlContext
     */
    public static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定范围内的用户访问行为数据
     *
     * @param sqlContext
     * @param taskParam
     * @return
     */
    public static JavaRDD<Row> getActionRDDByDataRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action " +
                "where date >= '" + startDate + "'" +
                " and date < '" + endDate + "' ";
        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();

    }

    /**
     * 将数据根据sessionId聚合，然后生成<sessionId,(sessionId=?|searchKeyWords=?|...)>
     * @param sqlContext
     * @param actionRDD
     * @return
     */
    public static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext,JavaRDD<Row> actionRDD){


        //将sessionId放置key位置，然后value放整行记录
        JavaPairRDD<String,Row> sessionid2actionRDD= actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });

        //根据sessionId分组
        JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD=sessionid2actionRDD.groupByKey();
        //将一个组中的session信息其他字段进行聚合拼接，并返回<userId,(sessionId=?|searchKeyWords=?|clickCateoryIds=?)>
        JavaPairRDD<Long,String> aggregatedRDD=sessionid2actionsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {

                String sessionId = stringIterableTuple2._1();
                Iterator<Row> iterator = stringIterableTuple2._2().iterator();

                StringBuffer searchWords = new StringBuffer("");
                StringBuffer clickCategory = new StringBuffer("");
                Long userId = null;

                while (iterator.hasNext()) {
                    Row row = iterator.next();

                    if (userId == null) {
                        userId = row.getLong(1);
                    }
                    String search_word = row.getString(5);
                    Object  click_category = row.get(6);

                    if (StringUtils.isNotEmpty(search_word)) {
                        if (!searchWords.toString().contains(search_word)) {
                            searchWords.append(search_word + ",");
                        }
                    }
                    if (click_category !=null) {
                        if (!clickCategory.toString().contains(String.valueOf(click_category))) {
                            clickCategory.append(click_category);
                        }
                    }


                }
                String searchKeyWords = StringUtils.trimComma(searchWords.toString());
                String clickCatgoryIds = StringUtils.trimComma(clickCategory.toString());


                String partAggInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" + Constants.FIELF_SEARCH_KEY_WORDS + "=" + searchKeyWords
                        + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCatgoryIds;

                return new Tuple2<Long, String>(userId, partAggInfo);
            }
        });
        //将用户信息表提取并与之前的RDD进行join ，并获取用户的对应字段进行拼接
        String sql ="select * from user_info";
        JavaRDD<Row> userInfoRDD =sqlContext.sql(sql).javaRDD();
        JavaPairRDD<String,String> userId2InfoRDD =userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {

                return new Tuple2<Long, Row>(row.getLong(0),row);
            }
        }).join(aggregatedRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Row, String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<Row, String>> longTuple2Tuple2) throws Exception {
                Long userId = longTuple2Tuple2._1();
                Row userRow = longTuple2Tuple2._2()._1();
                String partInfo = longTuple2Tuple2._2()._2();

                String sessionId = StringUtils.getFieldFromConcatString(partInfo, "\\|", Constants.FIELD_SESSION_ID);
                int age = userRow.getInt(3);
                String professional = userRow.getString(4);
                String city = userRow.getString(5);
                String sex = userRow.getString(6);
                String fullInfo = partInfo + "|" + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;
                return new Tuple2<String, String>(sessionId, fullInfo);
            }
        });



        return userId2InfoRDD;

    }

    /**
     * 过滤session数据
     * @param sessionId2AggInfoRDD
     * @return
     */
    public static JavaPairRDD<String,String> filterSession(JavaPairRDD<String,String> sessionId2AggInfoRDD,final JSONObject taskParams){
        //将参数拼接成串
        String startAge =ParamUtils.getParam(taskParams,Constants.PARAM_START_AGE);
        String endAge =ParamUtils.getParam(taskParams,Constants.PARAM_END_AGE);
        String professionals =ParamUtils.getParam(taskParams,Constants.PARAM_PROFESSIONALS);
        String cities =ParamUtils.getParam(taskParams,Constants.PARAM_CITY);
        String sex =ParamUtils.getParam(taskParams,Constants.PARAM_SEX);
        String keywords =ParamUtils.getParam(taskParams,Constants.PARAM_KEYWORDS);
        String catagoryIds=ParamUtils.getParam(taskParams,Constants.PARAM_CATEGORY_IDS);

        String _parameter =(startAge !=null ?Constants.PARAM_START_AGE+"="+startAge+"|" :"")
                +(endAge !=null ?Constants.PARAM_END_AGE+"="+endAge+"|" :"")
                +(professionals !=null ?Constants.PARAM_PROFESSIONALS+"="+professionals+"|" :"")
                +(cities !=null ?Constants.PARAM_CITY+"="+cities+"|" :"")
                +(sex !=null ?Constants.PARAM_SEX+"="+sex+"|" :"")
                +(keywords !=null ?Constants.PARAM_KEYWORDS+"="+keywords+"|" :"")
                +(catagoryIds !=null ?Constants.PARAM_CATEGORY_IDS+"="+catagoryIds+"|" :"");
        if(_parameter.endsWith("\\|")){
            _parameter=_parameter.substring(0,_parameter.length()-1);
        }
        final String  parameter =_parameter;
        //根据筛选参数进行过滤
        JavaPairRDD<String,String> filterSessionid2AggrInfoRDD = sessionId2AggInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                //首先从Tuple获取聚合数据
                String aggrInfo = v1._2();
                //按照年龄进行过滤

                if (!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,
                        Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
                    return false;
                }
                //按照职业过滤
                if(!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,
                        parameter,Constants.PARAM_PROFESSIONALS)){
                    return false;
                }
                //按照城市筛选
                if(!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,
                        parameter,Constants.PARAM_CITY)){
                    return false;
                }
                //按照性别筛选
                if(!ValidUtils.equal(aggrInfo,Constants.FIELD_SEX,
                        parameter,Constants.PARAM_SEX)){
                    return false;
                }
                //按照关键词进行筛选
                if(!ValidUtils.in(aggrInfo,Constants.FIELF_SEARCH_KEY_WORDS,
                        parameter,Constants.PARAM_KEYWORDS)){
                    return false;
                }
                //按照点击品类行为分类
                if(!ValidUtils.in(aggrInfo,Constants.FIELD_CLICK_CATEGORY_IDS,
                        parameter,Constants.PARAM_CATEGORY_IDS)){
                    return false;
                }
                    return true;
            }
        });


        return filterSessionid2AggrInfoRDD;
    }
}
