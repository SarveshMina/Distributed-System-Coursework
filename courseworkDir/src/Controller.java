import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;


public class Controller {
  private int port;
  private int replicationFactor;
  private List<Socket> dstores = new CopyOnWriteArrayList<>();
  private Map<String, FileDetails> fileToDstoresMap = new ConcurrentHashMap<>();
  private Map<Socket, Integer> dstoreDetails = new ConcurrentHashMap<>();
  private Map<String, Integer> ackCounts = new ConcurrentHashMap<>();
  private static final Logger logger = Logger.getLogger(Controller.class.getName());

  public Controller(int port, int replicationFactor) {
    this.port = port;
    this.replicationFactor = replicationFactor;
    configureLogger();
  }

  public void start() {
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      logger.log(Level.INFO, "Controller listening on port " + port);
      System.out.println("Controller listening on port " + port);
      while (true) {
        Socket socket = serverSocket.accept();
        new Thread(() -> handleConnection(socket)).start();
      }
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error starting Controller: " + e.getMessage());
      System.out.println("Error starting Controller: " + e.getMessage());
    }
  }

  private void configureLogger() {
    try {
      FileHandler fileHandler = new FileHandler("Controller.log", true);
      fileHandler.setFormatter(new SimpleFormatter());
      logger.addHandler(fileHandler);
      logger.setLevel(Level.FINE);
      logger.setUseParentHandlers(false);
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error setting up logger", e);
      System.out.println("Error setting up logger");
    }
  }

  private void handleConnection(Socket socket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      String message;
      while ((message = in.readLine()) != null) {
        if (message.trim().isEmpty()) {
          logger.log(Level.INFO, "Received empty message");
          System.out.println("Received empty message");
          continue; // Skip empty or malformed lines
        }
        logger.log(Level.INFO, "Received message: " + message);
        System.out.println("Received message: " + message);
        String[] parts = message.split(" ");
        try {
          if (parts[0].equals("JOIN")) {
            handleJoin(socket, out, message);
          } else if (parts[0].equals("STORE")) {
            handleStoreRequest(socket, message, out);
          } else if (parts[0].equals("STORE_ACK")) {
            handleStoreAck(message);
          } else if (parts[0].equals("LOAD")) {
            handleLoadRequest(socket, message, out);
          }
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Error handling message: " + message + " Error: " + e.getMessage());
          System.out.println("Error handling message: " + message + " Error: " + e.getMessage());
        }
      }
      logger.log(Level.INFO, "Connection closed by client: " + socket.getInetAddress().getHostAddress());
      System.out.println("Connection closed by client: " + socket.getInetAddress().getHostAddress());
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Failed to handle connection: " + e.getMessage());
      System.out.println("Failed to handle connection: " + e.getMessage());
    } finally {
      dstores.remove(socket);
    }
  }

  private void handleJoin(Socket socket, PrintWriter out, String message) {
    try {
      String[] parts = message.split(" ");
      int dstorePort = Integer.parseInt(parts[1]);
      dstoreDetails.put(socket, dstorePort);
      dstores.add(socket);
      out.println("ACK");
      logger.log(Level.INFO, "Dstore joined from: " + socket.getInetAddress().getHostAddress() + ":" + dstorePort);
      System.out.println("Dstore joined from: " + socket.getInetAddress().getHostAddress() + ":" + dstorePort);
    } catch (NumberFormatException e) {
      logger.log(Level.SEVERE, "Error parsing Dstore port: " + e.getMessage());
      System.out.println("Error parsing Dstore port: " + e.getMessage());
    }
  }

  private void handleStoreRequest(Socket clientSocket, String message, PrintWriter out) {
    String[] parts = message.split(" ");
    if (parts.length < 3) {
      out.println("ERROR_MALFORMED_COMMAND");
      return;
    }
    String filename = parts[1];
    int fileSize = Integer.parseInt(parts[2]);
    if (fileToDstoresMap.containsKey(filename)) {
      out.println("ERROR_FILE_ALREADY_EXISTS");
    } else if (dstores.size() < replicationFactor) {
      out.println("ERROR_NOT_ENOUGH_DSTORES");
    } else {
      List<Socket> selectedDstores = selectDstores();
      if (selectedDstores.size() < replicationFactor) {
        out.println("ERROR_NOT_ENOUGH_DSTORES");
      } else {
        fileToDstoresMap.put(filename, new FileDetails(new ArrayList<>(selectedDstores), fileSize));
        ackCounts.put(filename, replicationFactor);
        String response = "STORE_TO " + formatDstorePorts(selectedDstores);
        out.println(response);
      }
    }
  }

  private void handleStoreAck(String message) {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      logger.log(Level.SEVERE, "Malformed STORE_ACK message: " + message);
      return; // Malformed message
    }
    String filename = parts[1];
    Integer count = ackCounts.getOrDefault(filename, 0);
    if (count <= 1) { // If this was the last needed ACK
      ackCounts.remove(filename);
      FileDetails details = fileToDstoresMap.get(filename);
      if (details == null || details.getDstoreSockets().isEmpty()) {
        logger.log(Level.WARNING, "No Dstore sockets found for file: " + filename);
      } else {
        logger.log(Level.INFO, "STORE_COMPLETE for " + filename);
        System.out.println("STORE_COMPLETE for " + filename);
      }
    } else {
      ackCounts.put(filename, count - 1);
    }
  }

  private void handleLoadRequest(Socket clientSocket, String message, PrintWriter out) {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      out.println("ERROR_MALFORMED_COMMAND");
      logger.log(Level.SEVERE, "Malformed LOAD command: " + message);
      return;
    }

    String filename = parts[1];
    System.out.println("Processing LOAD for " + filename);
    FileDetails details = new FileDetails(fileToDstoresMap.get(filename).getDstoreSockets(), fileToDstoresMap.get(filename).getFileSize());
    if (details.getDstoreSockets().isEmpty()) {
      out.println("ERROR_FILE_DOES_NOT_EXIST");
    } else {
      Socket selectedDstore = details.getDstoreSockets().get(new Random().nextInt(details.getDstoreSockets().size()));
      Integer port = dstoreDetails.get(selectedDstore);
      if (port == null) {
        out.println("ERROR_DSTORE_UNAVAILABLE");
        return;
      }
      out.println("LOAD_FROM " + port + " " + details.getFileSize());
      logger.log(Level.INFO, "Sent LOAD_FROM for " + filename + " to client: LOAD_FROM " + port + " " + details.getFileSize());
    }
  }


  private List<Socket> selectDstores() {
    List<Socket> selected = new ArrayList<>(dstores);
    Collections.shuffle(selected);
    return selected.subList(0, Math.min(replicationFactor, selected.size()));
  }

  private String formatDstorePorts(List<Socket> dstores) {
    StringBuilder sb = new StringBuilder();
    for (Socket dstore : dstores) {
      Integer port = dstoreDetails.get(dstore);
      if (port != null) {
        sb.append(port).append(" ");
      }
    }
    return sb.toString().trim();
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("Usage: java Controller <cport> <R>");
      return;
    }
    int cport = Integer.parseInt(args[0]);
    int R = Integer.parseInt(args[1]);
    new Controller(cport, R).start();
  }
}