import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Controller {
  private int port;
  private int replicationFactor;
  private List<Socket> dstores = new CopyOnWriteArrayList<>();
  private Map<String, List<Socket>> fileToDstoresMap = new ConcurrentHashMap<>();
  private Map<Socket, Integer> dstoreDetails = new ConcurrentHashMap<>();
  private Map<String, Integer> ackCounts = new ConcurrentHashMap<>();

  public Controller(int port, int replicationFactor) {
    this.port = port;
    this.replicationFactor = replicationFactor;
  }

  public void start() {
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.out.println("Controller started on port " + port);
      while (true) {
        Socket socket = serverSocket.accept();
        new Thread(() -> handleConnection(socket)).start();
      }
    } catch (IOException e) {
      System.out.println("Error starting Controller: " + e.getMessage());
    }
  }

  private void handleConnection(Socket socket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      String message;
      while ((message = in.readLine()) != null) {
        if (message.trim().isEmpty()) {
          continue; // Skip empty or malformed lines
        }
        System.out.println("Received message: " + message);
        try {
          if (message.startsWith("JOIN")) {
            handleJoin(socket, out, message);
          } else if (message.startsWith("STORE")) {
            handleStoreRequest(socket, message, out);
          } else if (message.startsWith("STORE_ACK")) {
            handleStoreAck(message);
          }
        } catch (Exception e) {
          System.out.println("Error handling message: " + message + " Error: " + e.getMessage());
        }
      }
      System.out.println("Connection closed by client: " + socket.getInetAddress().getHostAddress());
    } catch (IOException e) {
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
      System.out.println("Dstore joined from: " + socket.getInetAddress().getHostAddress() + ":" + dstorePort);
    } catch (NumberFormatException e) {
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
    if (fileToDstoresMap.containsKey(filename)) {
      out.println("ERROR_FILE_ALREADY_EXISTS");
    } else if (dstores.size() < replicationFactor) {
      out.println("ERROR_NOT_ENOUGH_DSTORES");
    } else {
      List<Socket> selectedDstores = selectDstores();
      if (selectedDstores.size() < replicationFactor) {
        out.println("ERROR_NOT_ENOUGH_DSTORES");
      } else {
        String response = "STORE_TO " + formatDstorePorts(selectedDstores);
        out.println(response);
        fileToDstoresMap.put(filename, new ArrayList<>(selectedDstores));
        ackCounts.put(filename, replicationFactor);
        System.out.println("Sent STORE_TO for " + filename + " to ports: " + response);
      }
    }
  }

  private void handleStoreAck(String message) {
    String[] parts = message.split(" ");
    if (parts.length < 2) return; // Malformed message
    String filename = parts[1];
    ackCounts.computeIfPresent(filename, (key, val) -> val - 1);
    if (ackCounts.getOrDefault(filename, 0) <= 0) {
      fileToDstoresMap.remove(filename);
      ackCounts.remove(filename);
      System.out.println("STORE_COMPLETE for " + filename);
      // Here we would notify the client that the STORE operation is complete
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
