import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Coordinator {
    private final int port;
    private final int persistenceTime; // Temporally-bound persistence threshold
    private final Map<String, ParticipantInfo> participants = new ConcurrentHashMap<>();
    private final List<Message> messages = Collections.synchronizedList(new ArrayList<>());

    public Coordinator(int port, int persistenceTime) {
        this.port = port;
        this.persistenceTime = persistenceTime;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Coordinator started on port " + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.out.println("Error starting Coordinator: " + e.getMessage());
        }
    }

    private void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String data = in.readLine();
            if (data != null) {
                System.out.println("Received: " + data);
                processRequest(data);
                out.println("{\"status\": \"ack\"}");
            }
        } catch (IOException e) {
            System.out.println("Error handling client: " + e.getMessage());
        }
    }

    private void processRequest(String request) {
        String[] parts = request.split(" ");
        String command = parts[0];
        String participantId;

        switch (command) {
            case "register":
                if (parts.length != 4) {
                    System.out.println("Error: Invalid register format.");
                    return;
                }
                participantId = parts[1];
                String ip = parts[2];
                int threadBPort = Integer.parseInt(parts[3]);
                participants.put(participantId, new ParticipantInfo(participantId, ip, threadBPort, "online"));
                System.out.println(
                        "Participant " + participantId + " registered at IP " + ip + " and port " + threadBPort);

                broadcastSystemMessage("register " + participantId + " " + ip + " " + threadBPort); // ✅ NEW
                break;

            case "deregister":
                participantId = parts[1];
                participants.remove(participantId);
                System.out.println("Participant " + participantId + " deregistered.");

                broadcastSystemMessage("deregister " + participantId); // ✅ NEW
                break;

            case "disconnect":
                participantId = parts[1];
                ParticipantInfo p1 = participants.get(participantId);
                if (p1 != null) {
                    p1.setStatus("offline");
                    System.out.println("Participant " + participantId + " disconnected.");
                    broadcastSystemMessage("disconnect " + participantId); // ✅ NEW
                }
                break;

            case "reconnect":
                if (parts.length != 3) {
                    System.out.println("Error: Invalid reconnect format.");
                    return;
                }
                participantId = parts[1];
                int newPort = Integer.parseInt(parts[2]);
                ParticipantInfo p2 = participants.get(participantId);
                if (p2 != null) {
                    p2.setStatus("online");
                    p2.setPort(newPort);
                    System.out.println("Participant " + participantId + " reconnected.");
                    broadcastSystemMessage("reconnect " + participantId + " " + newPort); // ✅ NEW
                    sendMessagesToParticipant(participantId);
                }
                break;

            case "msend":
                if (parts.length < 3) {
                    System.out.println("Error: Invalid msend format.");
                    return;
                }
                participantId = parts[1];
                String message = String.join(" ", Arrays.copyOfRange(parts, 2, parts.length));
                Message newMessage = new Message(participantId, message, System.currentTimeMillis());
                messages.add(newMessage);
                System.out.println("Message from " + participantId + ": " + message);
                sendMessagesToOnlineParticipants();
                break;

            default:
                System.out.println("Error: Unknown command.");
        }
    }

    private void sendMessagesToParticipant(String participantId) {
        ParticipantInfo participant = participants.get(participantId);
        if (participant == null || !participant.getStatus().equals("online"))
            return;

        long currentTime = System.currentTimeMillis();
        long thresholdTime = currentTime - (persistenceTime * 1000);

        List<Message> eligibleMessages = messages.stream()
                .filter(msg -> msg.getTimestamp() > thresholdTime)
                .collect(Collectors.toList());

        for (Message message : eligibleMessages) {
            try (Socket socket = new Socket(participant.getIp(), participant.getPort());
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                out.println("msend " + message.getSenderId() + " " + message.getMessage());

            } catch (IOException e) {
                System.out.println("Error sending message to " + participantId + ": " + e.getMessage());
            }
        }
    }

    private void sendMessagesToOnlineParticipants() {
        for (ParticipantInfo participant : participants.values()) {
            if (participant.getStatus().equals("online")) {
                sendMessagesToParticipant(participant.getId());
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java Coordinator <config_file>");
            System.exit(1);
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(args[0]))) {
            int port = Integer.parseInt(reader.readLine().trim());
            int persistenceTime = Integer.parseInt(reader.readLine().trim());
            new Coordinator(port, persistenceTime).start();
        } catch (IOException e) {
            System.out.println("Error reading config file: " + e.getMessage());
        }
    }

    private void broadcastSystemMessage(String message) {
        for (ParticipantInfo participant : participants.values()) {
            if ("online".equals(participant.getStatus())) {
                try (Socket socket = new Socket(participant.getIp(), participant.getPort());
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                    out.println(message);
                } catch (IOException e) {
                    System.out
                            .println("Error sending system message to " + participant.getId() + ": " + e.getMessage());
                }
            }
        }
    }

}
