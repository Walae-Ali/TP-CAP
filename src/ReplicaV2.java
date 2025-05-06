import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ReplicaV2 {

    private final static String WRITE_EXCHANGE = "replication";
    private final static String READ_EXCHANGE = "read_fanout";  // Nouveau exchange pour Read All

    public static void main(String[] argv) throws Exception {
        if (argv.length != 1) {
            System.err.println("Usage: java ReplicaV2 <replica_id>");
            System.exit(1);
        }

        String replicaId = argv[0];
        String filename = "replica" + replicaId + ".txt";
        String writeQueue = "replica" + replicaId + "Queue";
        String readQueue = "replica" + replicaId + "_read_queue"; // propre à chaque replica

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 1. Exchange fanout pour écrire
        channel.exchangeDeclare(WRITE_EXCHANGE, "fanout");
        channel.queueDeclare(writeQueue, false, false, false, null);
        channel.queueBind(writeQueue, WRITE_EXCHANGE, "");

        // 2. Exchange fanout pour lire
        channel.exchangeDeclare(READ_EXCHANGE, "fanout");
        channel.queueDeclare(readQueue, false, false, false, null);
        channel.queueBind(readQueue, READ_EXCHANGE, "");

        System.out.println(" [*] Réplica " + replicaId + " prêt...");

        // === Callback pour les écritures ===
        DeliverCallback writeCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Réplica " + replicaId + " écrit : '" + message + "'");
            try (FileWriter writer = new FileWriter(filename, true)) {
                writer.write(message + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        // === Callback pour les lectures ===
        DeliverCallback readCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            String replyTo = delivery.getProperties().getReplyTo();

            if (message.equals("Read Last")) {
                String lastLine = lireDerniereLigne(filename);
                String response = "Replica " + replicaId + " => " + lastLine;
                channel.basicPublish("", replyTo, null, response.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [→] Réplica " + replicaId + " répond (last) : " + lastLine);
            }

            if (message.equals("Read All")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        channel.basicPublish("", replyTo, null, line.getBytes(StandardCharsets.UTF_8));
                        try {
                            Thread.sleep(50); // pour lisibilité
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    System.out.println(" [→] Réplica " + replicaId + " a envoyé toutes les lignes.");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        channel.basicConsume(writeQueue, true, writeCallback, consumerTag -> {});
        channel.basicConsume(readQueue, true, readCallback, consumerTag -> {});
    }

    private static String lireDerniereLigne(String filename) {
        String lastLine = "(vide)";
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lastLine = line;
            }
        } catch (IOException e) {
            lastLine = "(erreur)";
        }
        return lastLine;
    }
}
