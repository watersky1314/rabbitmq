package consumer.confirm;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

public class DirectConsumerReject {
    private final static String EXCHANGE_NAME = "confirm_log";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory();
        Connection con = cf.newConnection();
        Channel channel = con.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String routingKey = "error";
        String queueName = "confirm_queue";
        // 声明队列
        channel.queueDeclare(queueName, false, false, false, null);
        // 路由和队列绑定
        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        System.out.println("waiting message...");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, Charset.defaultCharset());
                System.out.println("accept " + envelope.getRoutingKey() + ":" + message);
                // envelope.getDeliveryTag():消费者每接收一下次就递增；
                // queue=true:消息返回队列重新投递消息者，false:消息从队列中移除
                this.getChannel().basicReject(envelope.getDeliveryTag(), true);

            }
        };
        channel.basicConsume(queueName, false, consumer);
    }
}
