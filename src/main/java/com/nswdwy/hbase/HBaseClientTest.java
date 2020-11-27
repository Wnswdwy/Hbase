import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;

public class HBaseClientTest {

    private static Connection connection;

    static {
        try {
            //通过配置类获取配置对象
            Configuration conf = HBaseConfiguration.create();
            //设置zk的地址
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //通过工厂类获取连接
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 测试获取连接
     */
    @Test
    public void testConnection(){
        System.out.println(connection);
    }

    //DDL操作：需要用到Admin对象
    /**
     * 测试命名空间的操作
     */
    @Test
    public void testNamespace() throws IOException {
        //获取Admin对象
        Admin admin = connection.getAdmin();
        //获取builder对象
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("atguigu");
        //创建命名空间的描述器对象
        NamespaceDescriptor namespaceDescriptor = builder.build();
        //创建命名空间
//        admin.createNamespace(namespaceDescriptor);
        //删除命名空间
        admin.deleteNamespace("atguigu");
        //关闭admin
        admin.close();
    }
}
