package consumer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

public class RecieveQ1 {

	private final static String QUEUE_NAME = "Q1";
	private static final String EXCHANGE_NAME = "E1";
	private final static String ROUTING_KEY = "R.K.1";


	public static void main(String[] argv) throws Exception {
		//recieveFromQueue();
		recieveFromExchange();
	}

	private static void recieveFromExchange() throws IOException, TimeoutException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
		ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

		channel.exchangeDeclare(EXCHANGE_NAME,BuiltinExchangeType.TOPIC);
		String queueName = channel.queueDeclare().getQueue();
		channel.queueBind(queueName, EXCHANGE_NAME, ROUTING_KEY);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DefaultConsumer consumer = new DefaultConsumer(channel){
    	@Override
		public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
				byte[] body) throws IOException {
			String message = new String(body, "UTF-8");
			System.out.println(" [x] Received in R1 '" + envelope.getRoutingKey() + "':'" + message + "'");
		}
	};
        channel.basicConsume(queueName, true, consumer);
        
	}

	private static void recieveFromQueue() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				System.out.println(" [x] Received from Q1 '" + message + "'");
			}
		};
		channel.basicConsume(QUEUE_NAME, true, consumer);		
	}
}
