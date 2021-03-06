package com.spark.study.spark;

import com.spark.study.constrants.Constants;
import com.spark.study.utils.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * session聚合统计Accumlator
 * Created by Administrator on 2018/2/11/011.
 */
public class SessionAggStatAccumulator  implements AccumulatorParam<String> {




    @Override
    public String addAccumulator(String s, String s2) {
        return add(s,s2);
    }



    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1,r2);
    }

    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT+"=0|"
                +Constants.TIME_PERIOD_1s_3s+"=0|"
                +Constants.TIME_PERIOD_4s_6s+"=0|"
                +Constants.TIME_PERIOD_7s_9s+"=0|"
                +Constants.TIME_PERIOD_10s_30s+"=0|"
                +Constants.TIME_PERIOD_30s_60s+"=0|"
                +Constants.TIME_PERIOD_1m_3m+"=0|"
                +Constants.TIME_PERIOD_3m_10m+"=0|"
                +Constants.TIME_PERIOD_10m_30m+"=0|"
                +Constants.TIME_PERIOD_30m+"=0|"
                +Constants.STEP_PERIOD_1_3+"=0|"
                +Constants.STEP_PERIOD_4_6+"=0|"
                +Constants.STEP_PERIOD_7_9+"=0|"
                +Constants.STEP_PERIOD_10_30+"=0|"
                +Constants.STEP_PERIOD_30_60+"=0|"
                +Constants.STEP_PERIOD_60+"=0|"
                ;
    }
    private String add(String v1,String v2 ){
        //校验 ：v1为空 返回v2
        if(StringUtils.isEmpty(v1)){
            return v2;
        }
        String oldValue =StringUtils.getFieldFromConcatString(v1,"\\|",v2);
        if(oldValue !=null){
            int newValue =Integer.valueOf(oldValue)+1;
            return StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue));
        }
        return null;
    }
}
