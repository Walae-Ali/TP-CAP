import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class ClientReaderV2 {

    private static final String READ_EXCHANGE = "read_fanout";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Déclarer l'exchange fanout pour lecture
        channel.exchangeDeclare(READ_EXCHANGE, "fanout");

        // Créer une file anonyme temporaire pour recevoir les réponses
        String replyQueue = channel.queueDeclare().getQueue();

        // Pour collecter toutes les lignes reçues
        List<String> allReceivedLines = Collections.synchronizedList(new ArrayList<>());

        // Callback pour réception ligne par ligne
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String line = new String(delivery.getBody(), StandardCharsets.UTF_8);
            allReceivedLines.add(line);
        };

        // Démarrer la consommation
        channel.basicConsume(replyQueue, true, deliverCallback, consumerTag -> {});

        // Envoyer la requête "Read All" à tous les réplicas
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .replyTo(replyQueue)
                .build();

        channel.basicPublish(READ_EXCHANGE, "", props, "Read All".getBytes(StandardCharsets.UTF_8));
        System.out.println("[x] Requête 'Read All' envoyée à tous les réplicas...");

        // Attendre suffisamment longtemps pour recevoir toutes les lignes
        Thread.sleep(5000);

        // Compter les occurrences de chaque ligne
        Map<String, Integer> lineCounts = new HashMap<>();
        for (String line : allReceivedLines) {
            lineCounts.put(line, lineCounts.getOrDefault(line, 0) + 1);
        }

        // Afficher uniquement les lignes qui apparaissent dans la majorité (≥ 2 réplicas)
        System.out.println("\nLignes majoritaires (présentes dans au moins 2 réplicas) :");
        for (Map.Entry<String, Integer> entry : lineCounts.entrySet()) {
            if (entry.getValue() >= 2) {
                System.out.println(entry.getKey());
            }
        }

        channel.close();
        connection.close();
    }
}

