import com.spark.study.conf.ConfigurationManager;


/**
 * Created by Administrator on 2018/1/25/025.
 */
public class ConfigurationManagerTest {


    public static void  main(String[] arg0){
        String key1 = ConfigurationManager.getProperty("testkey1");
        String key2 = ConfigurationManager.getProperty("testkey2");
        System.out.println(key1 +" || "+key2);

    }
}
