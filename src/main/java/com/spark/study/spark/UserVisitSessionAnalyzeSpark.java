package com.spark.study.spark;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.spark.study.conf.ConfigurationManager;
import com.spark.study.constrants.Constants;
import com.spark.study.dao.DAOFactory;
import com.spark.study.dao.ISessionAggrStatDAO;
import com.spark.study.dao.ITaskDAO;
import com.spark.study.dao.SessionRandomDAO;
import com.spark.study.dao.impl.ISessionAggrStatDAOImpl;
import com.spark.study.dao.impl.SessionRandomDAOImpl;
import com.spark.study.domain.SessionAggrStat;
import com.spark.study.domain.SessionRandomExtract;
import com.spark.study.domain.Task;
import com.spark.study.utils.*;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.*;

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

        //System.out.println(sessionid2AggrInfoRDD.count());

        //重构，同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator =sc.accumulator("",new SessionAggStatAccumulator());


        //针对session粒度的聚合数据，进行筛选参数进行指定的筛选
        JavaPairRDD<String ,String > filterSessionId2AggrInfoRDD =filterSession(sessionid2AggrInfoRDD, taskParam,sessionAggrStatAccumulator);
//        for(Tuple2<String ,String > tuple :filterSessionId2AggrInfoRDD.take(10)){
//            System.out.println(tuple._2());
//        }

        System.out.println(filterSessionId2AggrInfoRDD.count());

        randomExtractSession(task.getTaskId(), sessionid2AggrInfoRDD);






       // calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());
//        for(Tuple2<String,String> tuple : sessionid2AggrInfoRDD.take(1)){
//            System.out.println(tuple._2());
//        }
//        System.out.println(filterSessionId2AggrInfoRDD.count());
        //关闭上下文
        sc.close();

    }

    /**
     *
     * @param sessionId2AggrInfoRDD
     */
    private static void randomExtractSession(
            long taskId,
            JavaPairRDD<String, String> sessionId2AggrInfoRDD) {
        //第一步，计算每天每小时打的session数量，获取<yyyy-MM-dd_HH,sessionID>格式的RDD
        JavaPairRDD<String,String> time2sessionIdRDD = sessionId2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(
                    Tuple2<String, String> stringStringTuple2) throws Exception {
                String aggrInfo = stringStringTuple2._2();
                String sessionId = stringStringTuple2._1();
                String startTime = StringUtils.getFieldFromConcatString(
                        aggrInfo,"\\|",Constants.FIELD_START_TIME);

                return new Tuple2<String, String>(DateUtils.getDateHour(startTime),aggrInfo);
            }
        });
        //得到每天每小时的
        Map<String,Object> countMap =time2sessionIdRDD.countByKey();

        //第二步，使用按时间比例随机抽取算法
        //将<yyyy-MM-dd_HH,count>格式的map，转成<yyyy-MM-dd,<HH,count>>
        Map<String,Map<String,Long>>dayHourCountMap =new HashMap<>();
        for(Map.Entry<String,Object> countEntry : countMap.entrySet()){
            String dateHour = countEntry.getKey();
            String date =dateHour.split("_")[0];
            String hour =dateHour.split("_")[1];
            long count =Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String,Long> hourCountMap =dayHourCountMap.get(date);
            if(hourCountMap == null ){
                hourCountMap = new HashMap<>();
            }
            hourCountMap.put(hour,count);
            dayHourCountMap.put(date,hourCountMap);

        }
        //开始实现我们的按时间比例随机抽取算法
        int extractNumberPerDay =  100/dayHourCountMap.size();
        //<date,<hour,(3,5,30,102)>>
        Map<String,Map<String,List<Integer>>> dateHourExtractMap =
                new HashMap<>();
        Random random =new Random();

        for(Map.Entry<String,Map<String,Long>> dayHourCountEntry :dayHourCountMap.entrySet()){
            String date = dayHourCountEntry.getKey();
            Map<String,Long> hourCountMap =dayHourCountEntry.getValue();

            //计算session总数
            long sessionCount =0L;
            for(long houtCount :hourCountMap.values()){
                sessionCount +=houtCount;
            }

            Map<String,List<Integer>> hourExtractMap =dateHourExtractMap.get(date);
            if(hourExtractMap == null){
                hourExtractMap =new HashMap<>();
                dateHourExtractMap.put(date,hourExtractMap);
            }
            //遍历每个小时
            for(Map.Entry<String,Long> hourCountEntry :hourCountMap.entrySet()){
                String hour = hourCountEntry.getKey();
                long count =hourCountEntry.getValue();


                //计算每个小时的session数量，占据当天session数量的比例
                int hourExtractNumber = (int) (((double)count /(double)sessionCount)
                        * extractNumberPerDay);
                if(hourExtractNumber > count){
                    hourExtractNumber= (int) count;
                }
                //先获取当前小时的随机数List
                List<Integer> extractIndexList =hourExtractMap.get(hour);
                if(extractIndexList == null){
                    extractIndexList =new ArrayList<>();
                    hourExtractMap.put(hour,extractIndexList);
                }
                for(int i = 0 ; i < hourExtractNumber;i++){
                    int extractIndex =random.nextInt((int) count);
                    while(extractIndexList.contains(extractIndex)){
                        //重复就重新获取
                        extractIndex =random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }

            }


        }
        //第三部：遍历每天每小时的session
        //执行groupByKey算子，得到<dataHour,(session aggInfo)>
       JavaPairRDD<String,Iterable<String>> time2sessionsRDD = time2sessionIdRDD.groupByKey();

        JavaPairRDD<String,String>  extractSessionIdsRDD =time2sessionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                List<Tuple2<String,String>> extractSessionids =new ArrayList<>();

                String dateHour =stringIterableTuple2._1();
                String date =dateHour.split("_")[0];
                String hour =dateHour.split("_")[1];
                Iterator<String> iterator =stringIterableTuple2._2().iterator();

                List<Integer> extractIndexList =dateHourExtractMap.get(date).get(hour);

                SessionRandomDAO sessionRandomDAO =DAOFactory.getSessionRandomDAO();
                int index =0;
                while(iterator.hasNext()){
                    String sessionAggrInfo =iterator.next();
                    String sessionId =StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                    if(extractIndexList.contains(index)){
                        //写入mysql
                        SessionRandomExtract sre =new SessionRandomExtract();
                        sre.setTaskId(taskId);
                        sre.setStartTime(StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                        sre.setSessionId(sessionId);
                        sre.setSearchKeyWord(StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEY_WORDS));
                        sre.setClickCategoryids(StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
                        sessionRandomDAO.insert(sre);
                        //将sessionId 加入list
                        extractSessionids.add(new Tuple2<String, String>(sessionId,sessionId));
                    }

                    index++;
                }

                return extractSessionids;
            }
        });
        String sql ="select * from user_visit_action";

        extractSessionIdsRDD.join(sessionId2AggrInfoRDD);
        extractSessionIdsRDD.collect();

    }

    /**
     * 计算各个session占比，并写入MYSQL
     * @param value
     */
    private static void calculateAndPersistAggrStat(
            String value,
            long taskId) {
        System.out.println(value);
        //获取
        ISessionAggrStatDAO iSessionAggrStatDAO=DAOFactory.getISessionAggrStatDAO();
        //获取对应的字符串参数信息
        long session_count =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.SESSION_COUNT )) ;
        long visit_length_1s_3s =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.TIME_PERIOD_1s_3s )) ;
        long visit_length_4s_6s =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.TIME_PERIOD_4s_6s )) ;
        long visit_length_7s_9s =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.TIME_PERIOD_7s_9s )) ;
        long visit_length_10s_30s =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.TIME_PERIOD_10s_30s )) ;
        long visit_length_30s_60s =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.TIME_PERIOD_10s_30s )) ;
        long visit_length_1m_3m =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.TIME_PERIOD_1m_3m )) ;
        long visit_length_3m_10m =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.TIME_PERIOD_3m_10m )) ;
        long visit_length_10m_30m =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.TIME_PERIOD_10m_30m )) ;
        long visit_length_30m =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.TIME_PERIOD_30m )) ;
        long step_length_1_3 =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.STEP_PERIOD_1_3 )) ;
        long step_length_4_6 =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.STEP_PERIOD_4_6 )) ;
        long step_length_7_9 =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.STEP_PERIOD_7_9 )) ;
        long step_length_10_30 =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.STEP_PERIOD_10_30 )) ;
        long step_length_30_60 =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.STEP_PERIOD_30_60 )) ;
        long step_length_60 =Long.valueOf(StringUtils.getFieldFromConcatString(
                value , "\\|" , Constants.STEP_PERIOD_60 )) ;
        //计算访问时长占比
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s/(double)session_count,2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s/(double)session_count,2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s/(double)session_count,2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s/(double)session_count,2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s/(double)session_count,2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m/(double)session_count,2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m/(double)session_count,2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m/(double)session_count,2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m/(double)session_count,2);
        //计算访问步长占比
        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3/(double)session_count,2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6/(double)session_count,2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9/(double)session_count,2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30/(double)session_count,2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60/(double)session_count,2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60/(double)session_count,2);

        //封装分在domain里
        SessionAggrStat sessionAggrStat =new SessionAggrStat();
        sessionAggrStat.setTaskId(taskId);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        iSessionAggrStatDAO.insert(sessionAggrStat);
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
    public static JavaRDD<Row> getActionRDDByDataRange(
            SQLContext sqlContext,
            JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action " +
                "where date >= '" + startDate + "'" +
                " and date <= '" + endDate + "' ";
        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();

    }

    /**
     * 将数据根据sessionId聚合，然后生成<sessionId,(sessionId=?|searchKeyWords=?|...)>
     * @param sqlContext
     * @param actionRDD
     * @return
     */
    public static JavaPairRDD<String, String> aggregateBySession(
            SQLContext sqlContext,
            JavaRDD<Row> actionRDD){

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

                //session的开始时间
                Date startTime =null;
                Date endTime =null;
                //session 的访问不畅
                int stepLength =0;

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


                    //计算session的开始时间和结束时间
                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    if(startTime == null ){
                        startTime = actionTime;
                    }
                    if(endTime == null){
                        endTime = actionTime;
                    }

                    if(actionTime.after(endTime)){
                        endTime = actionTime;
                    }
                    if(actionTime.before(startTime)){
                        startTime = actionTime;
                    }

                    //计算session访问步长
                    stepLength++;
                }
                String searchKeyWords = StringUtils.trimComma(searchWords.toString());
                String clickCatgoryIds = StringUtils.trimComma(clickCategory.toString());

                //计算session访问时长（s）
                long visitLength = (endTime.getTime()-startTime.getTime())/1000;


                String partAggInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                        + Constants.FIELD_SEARCH_KEY_WORDS + "=" + searchKeyWords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCatgoryIds + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH +"=" + stepLength +"|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

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
    public static JavaPairRDD<String,String> filterSession(
            JavaPairRDD<String,String> sessionId2AggInfoRDD,
            final JSONObject taskParams,
            Accumulator<String> sessionAggrStatAccumulator){
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
                if(!ValidUtils.in(aggrInfo,Constants.FIELD_SEARCH_KEY_WORDS,
                        parameter,Constants.PARAM_KEYWORDS)){
                    return false;
                }
                //按照点击品类行为分类
                if(!ValidUtils.in(aggrInfo,Constants.FIELD_CLICK_CATEGORY_IDS,
                        parameter,Constants.PARAM_CATEGORY_IDS)){
                    return false;
                }
                //经过过滤条件，最后返回true，就说明是要保留的session
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                long visitLength =Long.valueOf(StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|" , Constants.FIELD_VISIT_LENGTH ));
                long stepLength =Long.valueOf(StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                //往累加器更新访问时长数据
                calculateVisitLength(visitLength);
                //往累加器更新访问步长数据
                calculateStepLength(stepLength);
                return true;
            }

            private void calculateVisitLength(long visitLength){
                if(visitLength >= 1 && visitLength <= 3 ){
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                }else if(visitLength >= 4 && visitLength <= 6 ){
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                }else if(visitLength >= 7 && visitLength <= 9 ){
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                }else if(visitLength >= 10 && visitLength <= 30 ){
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                }else if(visitLength > 30 && visitLength <= 60 ){
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                }else if(visitLength > 60 && visitLength <= 3*60 ){
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                }else if(visitLength > 3*60 && visitLength <= 10*60 ){
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                }else if(visitLength > 10*60 && visitLength <= 30*60 ){
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                }else if(visitLength > 30*60 ){
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }

            }
            private void calculateStepLength(long stepLength){
                if(stepLength >= 1 && stepLength <= 3 ){
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                }else if(stepLength >= 4 && stepLength <= 6 ){
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                }else if(stepLength >= 7 && stepLength <= 9 ){
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                }else if(stepLength >= 10 && stepLength <= 30 ){
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                }else if(stepLength > 30 && stepLength <= 60 ){
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                }else if(stepLength > 60  ){
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }

        });


        return filterSessionid2AggrInfoRDD;
    }
}
