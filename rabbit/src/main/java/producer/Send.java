package producer;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Send {
	private final static String QUEUE_1_NAME = "Q1";
	private final static String QUEUE_2_NAME = "Q2";
	private final static String EXCHANGE_1_NAME = "E1";
	private final static String EXCHANGE_2_NAME = "E2";
	private final static String ROUTING_KEY_1 = "R.K.1";
	private final static String ROUTING_KEY_2 = "R.K.2";

	public static void main(String[] args) throws IOException, TimeoutException {
		// sendOneToQueue();

		// sendTwoToQueues();

		sendOneToExchange();

	}

	private static void sendOneToExchange() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.exchangeDeclare(EXCHANGE_1_NAME, BuiltinExchangeType.TOPIC);

		String message = "Hello Exchange";

		channel.basicPublish(EXCHANGE_1_NAME, ROUTING_KEY_1, null, message.getBytes());
		System.out.println(" [x] Sent '" + ROUTING_KEY_1 + " " + message + "'");
		
		channel.basicPublish(EXCHANGE_1_NAME, ROUTING_KEY_2, null, message.getBytes());
		System.out.println(" [x] Sent '" + ROUTING_KEY_2 + " " + message + "'");


		channel.close();
		connection.close();
	}

	private static void sendTwoToQueues() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_1_NAME, false, false, false, null);
		channel.queueDeclare(QUEUE_2_NAME, false, false, false, null);

		boolean isFirstQueue = false;
		Scanner sc = new Scanner(System.in);
		while (sc.hasNext()) {
			String message = sc.next();
			if (isFirstQueue) {
				channel.basicPublish("", QUEUE_1_NAME, null, message.getBytes());
				System.out.println(" [x] Sent to Q1 '" + message + "'");
				isFirstQueue = !isFirstQueue;

			} else {
				channel.basicPublish("", QUEUE_2_NAME, null, message.getBytes());
				System.out.println(" [x] Sent to Q2 '" + message + "'");
				isFirstQueue = !isFirstQueue;
			}
		}
		channel.close();
		connection.close();
	}

	static void sendOneToQueue() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_1_NAME, false, false, false, null);

		String message = "Hello dharmam!";
		channel.basicPublish("", QUEUE_1_NAME, null, message.getBytes());
		System.out.println(" [x] Sent '" + message + "'");

		channel.close();
		connection.close();

	}
}
