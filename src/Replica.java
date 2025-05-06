import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Replica {

    private final static String EXCHANGE_NAME = "replication";
    private final static String READ_QUEUE = "read_queue";

    public static void main(String[] argv) throws Exception {
        if (argv.length != 1) {
            System.err.println("Usage: java Replica <replica_id>");
            System.exit(1);
        }

        String replicaId = argv[0];
        String filename = "replica" + replicaId + ".txt";
        String replicaQueue = "replica" + replicaId + "Queue";

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Déclare l'exchange fanout
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        // Déclare la file dédiée à ce Replica et la lie à l’exchange
        channel.queueDeclare(replicaQueue, false, false, false, null);
        channel.queueBind(replicaQueue, EXCHANGE_NAME, "");

        // Déclare la file partagée pour les requêtes de lecture
        channel.queueDeclare(READ_QUEUE, false, false, false, null);

        System.out.println("🟢 Réplica " + replicaId + " prêt à recevoir des messages...");

        // Callback pour gérer les messages d'écriture
        DeliverCallback writeCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("✍️  Réplica " + replicaId + " écrit : '" + message + "'");

            try (FileWriter writer = new FileWriter(filename, true)) {
                writer.write(message + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        // Callback pour gérer les requêtes de lecture
        DeliverCallback readCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            AMQP.BasicProperties props = delivery.getProperties();
            String replyTo = props.getReplyTo();

            if (message.equals("Read Last") && replyTo != null) {
                String lastLine = lireDerniereLigne(filename);
                String response = "Replica " + replicaId + " => " + lastLine;

                // Répondre au client via la file replyTo
                channel.basicPublish("", replyTo, null, response.getBytes(StandardCharsets.UTF_8));
                System.out.println("📤 Réplica " + replicaId + " a répondu : " + response);
            }
        };

        // Consommer les messages d'écriture de sa file dédiée
        channel.basicConsume(replicaQueue, true, writeCallback, consumerTag -> {});
        // Consommer les requêtes de lecture de la file partagée
        channel.basicConsume(READ_QUEUE, true, readCallback, consumerTag -> {});
    }

    // Lit la dernière ligne du fichier
    private static String lireDerniereLigne(String filename) {
        String lastLine = "(vide)";
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lastLine = line;
            }
        } catch (IOException e) {
            lastLine = "(erreur lecture)";
        }
        return lastLine;
    }
}
