import com.spark.study.dao.DAOFactory;
import com.spark.study.dao.ITaskDAO;
import com.spark.study.domain.Task;
import com.spark.study.utils.MockData;
import org.junit.Test;

/**
 * Created by Administrator on 2018/1/31/031.
 */
public class DAOTest {
    @Test
    public void daoTest(){
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task =taskDAO.findById(1);
        System.out.println(task.toString());
    }
    public void mockTest(){
        MockData data =new MockData();

    }
}
