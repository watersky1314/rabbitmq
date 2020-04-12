package consumer.normal;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

/**
 * fanout模式与路由无关，广播全接收
 */
public class FanoutConsumerError {
    private final static String EXCHANGE_NAME = "fanout_log";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory();
        Connection con = cf.newConnection();
        Channel channel = con.createChannel();
        String logLevel = "error";
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, logLevel);
        System.out.println("waiting message...");
        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) {
                String message =  new String(body, Charset.defaultCharset());
                System.out.println("accept message: " + message);
            }
        };
        channel.basicConsume(queueName,true,consumer);

    }
}
