package producer.confirm;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

/**
 * 发送方确认模式——同步模式,同步模式会引起性能下降，采用同步，不如使用事务机制，因此建议采用异步模式
 */
public class ProducerConfirm {
    private final static String EXCHANGE_NAME = "confirm_log";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory cf = new ConnectionFactory();
        Connection con = cf.newConnection();
        Channel channel = con.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        // 将信道设置为发送方确认
        channel.confirmSelect();
        String[] logLevels = {"error", "info", "warning"};
        System.out.println("waiting message sent...");
        for (int i = 0; i < 10; i++) {
            String message = logLevels[i % 3] + " hello rabbitMq" + (i + 1);
            channel.basicPublish(EXCHANGE_NAME, logLevels[i % 3], null, message.getBytes(Charset.defaultCharset()));
            if (channel.waitForConfirms()) {
                System.out.println("sent message: " + message);
            }
        }
        channel.close();
        con.close();
    }
}
