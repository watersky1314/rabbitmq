package consumer.normal;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

/**
 * direct模式，路由一对一匹配
 */
public class DirectConsumerError {
    private final static String EXCHANGE_NAME = "direct_log";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory();
        Connection con = cf.newConnection();
        Channel channel = con.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();//声明随机队列
        String logLevel = "error";
        channel.queueBind(queueName, EXCHANGE_NAME, logLevel);
        System.out.println("waiting message...");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) {
                String message = new String(body, Charset.defaultCharset());
                System.out.println("accept " + envelope.getRoutingKey() + ":" + message);
            }
        };
        channel.basicConsume(queueName, true, consumer);


    }
}
