import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class Participant {
    private String id;
    private String coordinatorIP;
    private int coordinatorPort;
    private String logFile;
    private int currentPort;
    private boolean isOnline;
    private ExecutorService executorService;
    private ServerSocket threadBSocket;

    public Participant(String id, String coordinatorIP, int coordinatorPort, String logFile) {
        this.id = id;
        this.coordinatorIP = coordinatorIP;
        this.coordinatorPort = coordinatorPort;
        this.logFile = logFile;
        this.isOnline = false;
        this.executorService = Executors.newFixedThreadPool(2);
    }

    public void startThreadB(int port) {
        executorService.submit(() -> {
            try {
                threadBSocket = new ServerSocket(port);
                this.currentPort = port;
                isOnline = true;
                while (isOnline) {
                    Socket clientSocket = threadBSocket.accept();
                    handleMulticastMessage(clientSocket);
                }
            } catch (IOException e) {
                if (isOnline) {
                    System.out.println("Thread-B error: " + e.getMessage());
                }
            }
        });
    }

    private void handleMulticastMessage(Socket clientSocket) {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile, true))
        ) {
            String message = in.readLine();
            if (message != null) {
                logWriter.write(message + "\n");
                logWriter.flush();
                System.out.println("Received and logged multicast message: " + message);
            }
        } catch (IOException e) {
            System.out.println("Error handling multicast message: " + e.getMessage());
        }
    }

    public void sendMessage(String message) {
        if (!isOnline) {
            System.out.println("Error: You cannot send messages because you're either disconnected or deregistered.");
            return;
        }

        try (
            Socket socket = new Socket(coordinatorIP, coordinatorPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            out.println("msend " + id + " " + message);
        } catch (IOException e) {
            System.out.println("Error sending message: " + e.getMessage());
        }
    }

    public void register(int port) {
        stopThreadB();
        startThreadB(port);

        try (
            Socket socket = new Socket(coordinatorIP, coordinatorPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            out.println("register " + id + " " + InetAddress.getLocalHost().getHostAddress() + " " + port);
            System.out.println("Registered participant " + id);
        } catch (IOException e) {
            System.out.println("Error registering: " + e.getMessage());
        }
    }

    public void reconnect(int port) {
        stopThreadB();
        startThreadB(port);

        try (
            Socket socket = new Socket(coordinatorIP, coordinatorPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            out.println("reconnect " + id + " " + port);
            System.out.println("Participant " + id + " reconnected.");
        } catch (IOException e) {
            System.out.println("Error reconnecting: " + e.getMessage());
        }
    }

    public void disconnect() {
        stopThreadB();

        try (
            Socket socket = new Socket(coordinatorIP, coordinatorPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            out.println("disconnect " + id);
            System.out.println("Participant " + id + " disconnected.");
        } catch (IOException e) {
            System.out.println("Error disconnecting: " + e.getMessage());
        }
    }

    public void deregister() {
        stopThreadB();

        try (
            Socket socket = new Socket(coordinatorIP, coordinatorPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            out.println("deregister " + id);
            System.out.println("Deregistered participant " + id);
        } catch (IOException e) {
            System.out.println("Error deregistering: " + e.getMessage());
        }
    }

    private void stopThreadB() {
        isOnline = false;
        if (threadBSocket != null && !threadBSocket.isClosed()) {
            try {
                threadBSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing thread-B socket: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java Participant <config_file>");
            System.exit(1);
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(args[0]))) {
            String id = reader.readLine().trim();
            String logFile = reader.readLine().trim();
            String[] coordParts = reader.readLine().trim().split(" ");
            String coordinatorIP = coordParts[0];
            int coordinatorPort = Integer.parseInt(coordParts[1]);

            Participant participant = new Participant(id, coordinatorIP, coordinatorPort, logFile);
            System.out.println("Participant " + id + " connecting to Coordinator at " + coordinatorIP + ":" + coordinatorPort);

            BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                System.out.print("Enter command (register <port>/reconnect <port>/msend/disconnect/deregister/exit): ");
                String input = console.readLine();
                if (input == null || input.trim().isEmpty()) continue;

                String[] parts = input.trim().split(" ");
                String command = parts[0];

                switch (command) {
                    case "register":
                        if (parts.length == 2) participant.register(Integer.parseInt(parts[1]));
                        else System.out.println("Usage: register <port>");
                        break;
                    case "reconnect":
                        if (parts.length == 2) participant.reconnect(Integer.parseInt(parts[1]));
                        else System.out.println("Usage: reconnect <port>");
                        break;
                    case "disconnect":
                        participant.disconnect();
                        break;
                    case "deregister":
                        participant.deregister();
                        break;
                    case "msend":
                        System.out.print("Enter message: ");
                        String msg = console.readLine();
                        participant.sendMessage(msg.trim());
                        break;
                    case "exit":
                        System.out.println("Exiting.");
                        participant.executorService.shutdownNow();
                        return;
                    default:
                        System.out.println("Invalid command.");
                }
            }
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
