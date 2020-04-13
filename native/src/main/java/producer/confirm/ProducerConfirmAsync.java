package producer.confirm;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

/**
 * 发送方确认模式——异步模式
 */
public class ProducerConfirmAsync {
    private final static String EXCHANGE_NAME = "confirm_log";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory cf = new ConnectionFactory();
        Connection con = cf.newConnection();
        // 连接关闭监听，一般用于重连机制
        con.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {

            }
        });
        Channel channel = con.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        // 将信道设置为发送方确认
        channel.confirmSelect();
        // 信道关闭监听
        channel.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {

            }
        });
        // deliveryTag:代表消息在信道中的唯一一次投递，单调递增
        // multiple:是否批处理，默认false
        // 已被投递的消息监听
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) {
                System.out.println("Ack deliveryTag=" + deliveryTag + ",multiple=" + multiple);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) {
                System.out.println("Ack deliveryTag=" + deliveryTag + ",multiple=" + multiple);
            }
        });

        // 未被投递到队列的消息监听
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey,
                                     AMQP.BasicProperties properties, byte[] body) {
                System.out.println("replyCode:" + replyCode);
                System.out.println("replyText:" + replyText);
                System.out.println("exchange:" + exchange);
                System.out.println("routingKey:" + routingKey);
                System.out.println("message:" + new String(body));
            }
        });
        String[] logLevels = {"error", "info", "warning"};
        System.out.println("waiting message sent...");
        // mandatory参数为true,当投递消息无法找到合适的消息队列，则返回生成者；false为缺省，丢弃消息
        for (int i = 0; i < 3; i++) {
            String message =  " hello rabbitMq" + (i + 1);
            channel.basicPublish(EXCHANGE_NAME, logLevels[i], true, null,
                    message.getBytes(Charset.defaultCharset()));
            System.out.println("---------------------------------------------------");
            System.out.println("sent message: [" +logLevels[i] +"]" + message);
            Thread.sleep(200);
        }
    }
}
