import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

public class Controller {
  private int port;
  private int replicationFactor;
  private List<Socket> dstores = new CopyOnWriteArrayList<>();
  private Index index = new Index();
  private Map<Socket, Integer> dstoreDetails = new ConcurrentHashMap<>();
  private Map<String, Integer> ackCounts = new ConcurrentHashMap<>();
  private Map<String, Long> storeStartTimes = new ConcurrentHashMap<>();
  private ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);
  private static final Logger logger = Logger.getLogger(Controller.class.getName());
  private int timeout;
  private int rebalance_period;

  public Controller(int port, int replicationFactor, int timeout, int rebalance_period) {
    this.port = port;
    this.replicationFactor = replicationFactor;
    this.timeout = timeout;
    this.rebalance_period = rebalance_period;
    configureLogger();
    startTimeoutCheck();
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

  private void startTimeoutCheck() {
    timeoutScheduler.scheduleAtFixedRate(() -> {
      long currentTime = System.currentTimeMillis();
      storeStartTimes.forEach((filename, startTime) -> {
        if (currentTime - startTime > timeout && ackCounts.containsKey(filename) && ackCounts.get(filename) > 0) {
          ackCounts.remove(filename);
          index.removeFile(filename);
          logger.log(Level.WARNING, "Timeout expired for STORE operation of file: " + filename);
        }
      });
    }, 0, 1, TimeUnit.SECONDS);
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

  private void handleConnection(Socket socket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      String message;
      while ((message = in.readLine()) != null) {
        if (message.trim().isEmpty()) {
          logger.log(Level.INFO, "Received empty message");
          continue;
        }
        logger.log(Level.INFO, "Received message: " + message);
        System.out.println("Received message: " + message);
        String[] parts = message.split(" ");
        try {
          switch (parts[0]) {
            case "JOIN":
              handleJoin(socket, out, message);
              break;
            case "STORE":
              handleStoreRequest(socket, message, out);
              break;
            case "STORE_ACK":
              handleStoreAck(message);
              break;
            case "LOAD":
              handleLoadRequest(socket, message, out);
              break;
            case "RELOAD":
              handleReloadRequest(socket, message, out);
              break;
            case "REMOVE":
              handleRemoveRequest(socket, message, out);
              break;
            case "REMOVE_ACK":
              handleRemoveAck(message);
              break;
          }
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Error handling message: " + message + " Error: " + e.getMessage());
        }
      }
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Failed to handle connection: " + e.getMessage());
    } finally {
      dstores.remove(socket);
    }
  }


  private void handleJoin(Socket socket, PrintWriter out, String message) {
    String[] parts = message.split(" ");
    int dstorePort = Integer.parseInt(parts[1]);
    dstoreDetails.put(socket, dstorePort);
    dstores.add(socket);
    out.println("ACK");
    System.out.println("Dstore joined from: " + dstorePort);
    logger.log(Level.INFO, "Dstore joined from: " + socket.getInetAddress().getHostAddress() + ":" + dstorePort);
  }

  private void handleStoreRequest(Socket clientSocket, String message, PrintWriter out) {
    String[] parts = message.split(" ");
    if (parts.length < 3) {
      out.println("ERROR_MALFORMED_COMMAND");
      return;
    }
    String filename = parts[1];
    int fileSize = Integer.parseInt(parts[2]);

    if (index.getFileState(filename) != null && !index.getFileState(filename).getStatus().equals("COMPLETE")) {
      out.println("ERROR_FILE_ALREADY_EXISTS");
    } else if (dstores.size() < replicationFactor) {
      out.println("ERROR_NOT_ENOUGH_DSTORES");
    } else {
      List<Socket> selectedDstores = selectDstores();
      if (selectedDstores.size() < replicationFactor) {
        out.println("ERROR_NOT_ENOUGH_DSTORES");
      } else {
        index.addFile(filename, new ArrayList<>(selectedDstores), fileSize);
        ackCounts.put(filename, replicationFactor);
        storeStartTimes.put(filename, System.currentTimeMillis());
        String response = "STORE_TO " + formatDstorePorts(selectedDstores);
        logger.log(Level.INFO, "Storing file: " + filename + " to Dstores: " + formatDstorePorts(selectedDstores));
        System.out.println("Storing file: " + filename + " to Dstores: " + formatDstorePorts(selectedDstores));
        out.println(response);
      }
    }
  }

  private void handleLoadRequest(Socket clientSocket, String message, PrintWriter out) {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      out.println("ERROR_MALFORMED_COMMAND");
      return;
    }
    String filename = parts[1];
    Index.FileState fileState = index.getFileState(filename);
    if (fileState == null) {
      out.println("ERROR_FILE_DOES_NOT_EXIST");
    } else if (fileState.getDstoreSockets().isEmpty() || dstores.size() < replicationFactor) {
      out.println("ERROR_NOT_ENOUGH_DSTORES");
    } else {
      Socket selectedDstore = selectDstoreForLoad(fileState.getDstoreSockets());
      if (selectedDstore == null) {
        out.println("ERROR_LOAD");
      } else {
        Integer port = dstoreDetails.get(selectedDstore);
        if (port == null) {
          out.println("ERROR_DSTORE_UNAVAILABLE");
        } else {
          out.println("LOAD_FROM " + port + " " + fileState.getFileSize());
        }
      }
    }
  }

  private void handleReloadRequest(Socket clientSocket, String message, PrintWriter out) {
    String[] parts = message.split(" ");
    if (parts.length < 3) {
      out.println("ERROR_MALFORMED_COMMAND");
      return;
    }
    String filename = parts[1];
    int failedPort = Integer.parseInt(parts[2]); // Assuming the failed port is sent as part of the command
    Index.FileState fileState = index.getFileState(filename);
    if (fileState == null) {
      out.println("ERROR_FILE_DOES_NOT_EXIST");
    } else {
      List<Socket> dstoreSockets = new ArrayList<>(fileState.getDstoreSockets()); // Copy to modify
      dstoreSockets.removeIf(socket -> dstoreDetails.get(socket) != null && dstoreDetails.get(socket) == failedPort);

      if (dstoreSockets.isEmpty() || dstores.size() < replicationFactor) {
        out.println("ERROR_NOT_ENOUGH_DSTORES");
      } else {
        Socket selectedDstore = selectDstoreForLoad(dstoreSockets);
        if (selectedDstore == null) {
          out.println("ERROR_LOAD");
        } else {
          Integer port = dstoreDetails.get(selectedDstore);
          if (port == null) {
            out.println("ERROR_DSTORE_UNAVAILABLE");
          } else {
            out.println("LOAD_FROM " + port + " " + fileState.getFileSize());
          }
        }
      }
    }
  }


  private void handleRemoveRequest(Socket clientSocket, String message, PrintWriter out) throws IOException {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      out.println("ERROR_MALFORMED_COMMAND");
      return;
    }
    String filename = parts[1];
    Index.FileState fileState = index.getFileState(filename);
    if (fileState == null) {
      out.println("ERROR_FILE_DOES_NOT_EXIST");
    } else {
      // Set the file state to removing
      fileState.setStatus("REMOVING");
      List<Socket> dstoreSockets = fileState.getDstoreSockets();
      for (Socket dstore : dstoreSockets) {
        PrintWriter dstoreOut = new PrintWriter(dstore.getOutputStream(), true);
        dstoreOut.println("REMOVE " + filename);
      }
      ackCounts.put(filename, dstoreSockets.size());
    }
  }

  // Method to handle REMOVE_ACK from Dstores
  private void handleRemoveAck(String message) {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      logger.log(Level.SEVERE, "Malformed REMOVE_ACK message: " + message);
      return;
    }
    String filename = parts[1];
    Integer count = ackCounts.getOrDefault(filename, 0);
    if (count <= 1) {
      ackCounts.remove(filename);
      index.removeFile(filename);
      index.getFileState(filename).getDstoreSockets().forEach(socket -> {
        try {
          PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
          out.println("REMOVE_COMPLETE " + filename);
        } catch (IOException e) {
          logger.log(Level.SEVERE, "Error sending REMOVE_COMPLETE to Dstore", e);
        }
      });
      logger.log(Level.INFO, "REMOVE_COMPLETE for " + filename);
    } else {
      ackCounts.put(filename, count - 1);
    }
  }

  private void handleStoreAck(String message) {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      logger.log(Level.SEVERE, "Malformed STORE_ACK message: " + message);
      return;
    }
    String filename = parts[1];
    Integer count = ackCounts.getOrDefault(filename, 0);
    if (count <= 1) {
      ackCounts.remove(filename);
      index.markFileAsComplete(filename);
      storeStartTimes.remove(filename);
      logger.log(Level.INFO, "STORE_COMPLETE for " + filename);
      System.out.println("STORE_COMPLETE for " + filename);
    } else {
      ackCounts.put(filename, count - 1);
    }
  }

  private Socket selectDstoreForLoad(List<Socket> dstoreSockets) {
    if (dstoreSockets == null || dstoreSockets.isEmpty()) return null;
    // Randomly pick one Dstore that has the file
    return dstoreSockets.get(new Random().nextInt(dstoreSockets.size()));
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
    if (args.length < 4) {
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
