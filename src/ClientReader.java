import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class ClientReader {
    private final static String READ_QUEUE = "read_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Création d'une file de réponse temporaire (exclusive, auto-supprimée)
        String replyQueue = channel.queueDeclare("", false, true, true, null).getQueue();

        // Préparer les propriétés avec le champ "replyTo"
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .replyTo(replyQueue)
                .build();

        // Envoi de la requête dans read_queue
        channel.basicPublish("", READ_QUEUE, props, "Read Last".getBytes(StandardCharsets.UTF_8));
        System.out.println("[→] Requête envoyée avec replyTo = " + replyQueue);

        // Attente de la première réponse
        DeliverCallback callback = (consumerTag, delivery) -> {
            String response = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[✓] Réponse reçue : " + response);

            // Fermeture propre
            try {
                channel.close();
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
            connection.close();
            System.out.println("✅ Lecture terminée proprement.");
        };

        // Écoute sur la file temporaire
        channel.basicConsume(replyQueue, true, callback, consumerTag -> {});
    }
}
