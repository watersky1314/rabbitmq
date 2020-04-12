package producer.confirm;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class DirectProducer {
    private final static String EXCHANGE_NAME = "confirm_log";

    public static void main(String[] args) throws IOException, TimeoutException {
        // 创建连接工厂
        ConnectionFactory cf = new ConnectionFactory();
        // 创建连接
        Connection con = cf.newConnection();
        // 创建信道
        Channel channel = con.createChannel();
        // 声明交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String logLevel = "error";
        for (int i = 0; i < 10; i++) {
            String message = logLevel + ":" + "hello rabbitmq"+(i+1);
            channel.basicPublish(EXCHANGE_NAME, logLevel, null, message.getBytes());
            System.out.println("send message:" + message);
        }
        channel.close();
        con.close();
    }
}
