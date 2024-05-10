import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Controller {
  private int port;
  private int replicationFactor;
  private int timeout;
  private int rebalancePeriod;
  private List<Socket> dstores = new ArrayList<>();
  private Map<Socket, Integer> dstoreDetails = new ConcurrentHashMap<>();

  public Controller(int port, int replicationFactor, int timeout, int rebalancePeriod) {
    this.port = port;
    this.replicationFactor = replicationFactor;
    this.timeout = timeout;
    this.rebalancePeriod = rebalancePeriod;
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
      String message = in.readLine();
      if (message != null && message.startsWith("JOIN")) {
        handleJoin(socket, out, message);
      }
    } catch (IOException e) {
      System.out.println("Failed to handle connection: " + e.getMessage());
    }
  }

  private void handleJoin(Socket socket, PrintWriter out, String message) {
    try {
      String[] parts = message.split(" ");
      if (parts.length == 2) {
        int dstorePort = Integer.parseInt(parts[1]);
        dstoreDetails.put(socket, dstorePort);
        dstores.add(socket);
        out.println("ACK");
        System.out.println("Dstore joined from: " + socket.getInetAddress().getHostAddress() + ":" + dstorePort);
      } else {
        System.out.println("Malformed JOIN message: " + message);
      }
    } catch (NumberFormatException e) {
      System.out.println("Error parsing Dstore port: " + e.getMessage());
    }
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: java Controller <cport> <R> <timeout> <rebalance_period>");
      return;
    }
    int cport = Integer.parseInt(args[0]);
    int R = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    int rebalance_period = Integer.parseInt(args[3]);
    new Controller(cport, R, timeout, rebalance_period).start();
  }
}
