package producer.normal;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class DirectProducer {

    private final static String EXCHANGE_NAME = "direct_log";
    public static void main(String[] args) throws IOException, TimeoutException {
        // 创建连接工厂
        ConnectionFactory cf = new ConnectionFactory();
        // 工厂属性配置，列举如下几个，其他属性自行点击ConnectionFactory进去查看
        cf.setHost("127.0.0.1");// ip
        cf.setPort(5672);// 端口
        cf.setUsername("guest");// 用户名
        cf.setPassword("guest");// 密码
        // 创建连接
        Connection con = cf.newConnection();
        // 创建信道
        Channel channel = con.createChannel();
        // 声明交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String[] logLevels = {"info", "error", "warning"};
        for (int i = 0; i < logLevels.length; i++) {
            String logLevel = logLevels[i];
            String message = logLevel + ":" + "hello rabbitmq";
            channel.basicPublish(EXCHANGE_NAME, logLevel, null, message.getBytes());
            System.out.println("send message:" + message);
        }
        channel.close();
        con.close();
    }
}
