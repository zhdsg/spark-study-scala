package com.spark.study.utils;

import java.math.BigDecimal;

/**
 * 数字格工具类
 * @author Administrator
 *
 */
public class NumberUtils {

	/**
	 * 格式化小�?
	 * @param str 字符�?
	 * @param scale 四舍五入的位�?
	 * @return 格式化小�?
	 */
	public static double formatDouble(double num, int scale) {
		BigDecimal bd = new BigDecimal(num);  
		return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
	
}
