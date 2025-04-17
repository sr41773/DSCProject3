import java.io.*;
import java.net.*;
import java.util.Date;
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
                //threadBSocket = new ServerSocket(port);
                ServerSocket tmp = new ServerSocket();
                tmp.setReuseAddress(true);
                tmp.bind(new InetSocketAddress(port));
                threadBSocket = tmp;

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
                String timestampedMessage = "[" + new Date() + "] " + message;
                logWriter.write(timestampedMessage + "\n");
                logWriter.flush();
                System.out.println("Received and logged multicast message: " + timestampedMessage);
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
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            out.println("msend " + id + " " + message);
            in.readLine();
        } catch (IOException e) {
            System.out.println("Error sending message: " + e.getMessage());
        }
    }

    public void register(int port) {
        stopThreadB();
        startThreadB(port);

        try (
            Socket socket = new Socket(coordinatorIP, coordinatorPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            out.println("register " + id + " " + InetAddress.getLocalHost().getHostAddress() + " " + port);
            in.readLine();
            isOnline = true;
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
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            //out.println("reconnect " + id + " " + port);
            out.println("reconnect " + id + " " + InetAddress.getLocalHost().getHostAddress() + " " + port);
            in.readLine();  
            isOnline = true;
            System.out.println("Participant " + id + " reconnected.");
        } catch (IOException e) {
            System.out.println("Error reconnecting: " + e.getMessage());
        }
    }

    public void disconnect() {
        stopThreadB();

        try (
            Socket socket = new Socket(coordinatorIP, coordinatorPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            out.println("disconnect " + id);
            in.readLine();
            isOnline = false;
            System.out.println("Participant " + id + " disconnected.");

            // Log the event
            try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile, true))) {
                logWriter.write("[" + new Date() + "] disconnect " + id + "\n");
                logWriter.flush();
            }
        } catch (IOException e) {
            System.out.println("Error disconnecting: " + e.getMessage());
        }
    }

    public void deregister() {
        stopThreadB();

        try (
            Socket socket = new Socket(coordinatorIP, coordinatorPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            out.println("deregister " + id);
            in.readLine();
            isOnline = false;
            System.out.println("Deregistered participant " + id);

            // Log the event
            try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile, true))) {
                logWriter.write("[" + new Date() + "] deregister " + id + "\n");
                logWriter.flush();
            }
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
        threadBSocket = null;
        try { 
            Thread.sleep(200); 
        } catch (InterruptedException ignored) {
            // Ignore the exception
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
                        participant.deregister(); // Ensure the participant deregisters before exiting
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
