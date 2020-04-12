package consumer.confirm;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

public class DirectConsumerAck {
    private final static String EXCHANGE_NAME = "confirm_log";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory();
        Connection con = cf.newConnection();
        Channel channel = con.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String routingKey = "error";
        // 声明队列
        String queueName = "confirm_queue";
        channel.queueDeclare(queueName, false, false, false, null);
        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        System.out.println("waiting message...");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, Charset.defaultCharset());
                System.out.println("accept " + envelope.getRoutingKey() + ":" + message);
                // envelope.getDeliveryTag():消费者每接收一下次就递增；
                // multiple=true:批量确认，false:不批量确认
                this.getChannel().basicAck(envelope.getDeliveryTag(), false);

            }
        };
        channel.basicConsume(queueName, false, consumer);
    }
}
