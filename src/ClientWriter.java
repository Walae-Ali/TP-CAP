import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class ClientWriter {

    private final static String EXCHANGE_NAME = "replication";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Déclare une exchange de type fanout
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.println("Entrez la ligne à écrire (ou 'exit' pour quitter) : ");
                String line = scanner.nextLine();
                if (line.equalsIgnoreCase("exit")) break;

                // Envoi du message à l'exchange (et non à une queue)
                channel.basicPublish(EXCHANGE_NAME, "", null, line.getBytes("UTF-8"));
                System.out.println("[x] Message envoyé : '" + line + "'");
            }
        }
    }
}
