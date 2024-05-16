import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
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
  private Map<String, Socket> storeRequestClients = new ConcurrentHashMap<>();
  private Map<String, Long> storeStartTimes = new ConcurrentHashMap<>();
  private Map<String, Socket> removeRequestClients = new ConcurrentHashMap<>();
  private ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);
  private static final Logger logger = Logger.getLogger(Controller.class.getName());
  private int timeout;
  private int rebalance_period;
  private final Map<String, ReentrantLock> fileLocks = new ConcurrentHashMap<>();

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
          synchronized (this) {
            ackCounts.remove(filename);
            index.removeFile(filename);
            fileLocks.remove(filename);
            logger.log(Level.WARNING, "Timeout expired for STORE operation of file: " + filename);
          }
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
            case "LIST":
              handleListRequest(socket, out);
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

  private void handleRemoveRequest(Socket clientSocket, String message, PrintWriter out) {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      logger.log(Level.SEVERE, "Malformed REMOVE message: " + message);
      return;
    }
    String filename = parts[1];

    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      Index.FileState fileState = index.getFileState(filename);
      if (fileState == null || "IN_PROGRESS".equals(fileState.getStatus())) {
        out.println("ERROR_FILE_DOES_NOT_EXIST");
      } else {
        List<Socket> dstoreSockets = new ArrayList<>(fileState.getDstoreSockets());
        if (dstoreSockets.isEmpty() || dstores.size() < replicationFactor) {
          out.println("ERROR_NOT_ENOUGH_DSTORES");
        } else {
          removeRequestClients.put(filename, clientSocket);  // Track client socket
          ackCounts.put(filename, dstoreSockets.size());     // Initialize ack count
          index.markFileInProgress(filename);                // Mark file as in-progress
          for (Socket dstore : dstoreSockets) {
            Integer port = dstoreDetails.get(dstore);
            if (port != null) {
              try {
                PrintWriter dstoreOut = new PrintWriter(dstore.getOutputStream(), true);
                dstoreOut.println("REMOVE " + filename);
              } catch (IOException e) {
                logger.log(Level.SEVERE, "Error sending REMOVE command to Dstore: " + e.getMessage());
              }
            }
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void handleRemoveAck(String message) {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      logger.log(Level.SEVERE, "Malformed REMOVE_ACK message: " + message);
      return;
    }
    String filename = parts[1];
    synchronized (this) {
      Integer count = ackCounts.getOrDefault(filename, 0);
      if (count <= 1) {
        ackCounts.remove(filename);
        index.removeFile(filename);
        fileLocks.remove(filename);
        logger.log(Level.INFO, "REMOVE_COMPLETE for " + filename);
        System.out.println("REMOVE_COMPLETE for " + filename);

        // Notify the client
        Socket clientSocket = removeRequestClients.remove(filename);
        if (clientSocket != null) {
          try {
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            out.println("REMOVE_COMPLETE");
          } catch (IOException e) {
            logger.log(Level.SEVERE, "Error sending REMOVE_COMPLETE to client: " + e.getMessage());
          }
        }
      } else {
        ackCounts.put(filename, count - 1);
      }
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
      logger.log(Level.SEVERE, "Malformed STORE message: " + message);
      return;
    }
    String filename = parts[1];
    int fileSize = Integer.parseInt(parts[2]);

    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      Index.FileState fileState = index.getFileState(filename);
      if (fileState != null && "COMPLETE".equals(fileState.getStatus())) {
        System.out.println("File Already Exists");
        out.println("ERROR_FILE_ALREADY_EXISTS");
      } else if (dstores.size() < replicationFactor) {
        System.out.println("Not enough Dstores");
        out.println("ERROR_NOT_ENOUGH_DSTORES");
      } else {
        List<Socket> selectedDstores = selectDstores();
        if (selectedDstores.size() < replicationFactor) {
          out.println("ERROR_NOT_ENOUGH_DSTORES");
        } else {
          if (fileState == null) {
            index.addFile(filename, selectedDstores, fileSize);
          } else {
            index.markFileInProgress(filename);
            fileState.setDstoreSockets(selectedDstores);
            fileState.setFileSize(fileSize);
          }
          ackCounts.put(filename, replicationFactor);
          storeStartTimes.put(filename, System.currentTimeMillis());
          storeRequestClients.put(filename, clientSocket);  // Track client socket
          String response = "STORE_TO " + formatDstorePorts(selectedDstores);
          logger.log(Level.INFO, "Storing file: " + filename + " to Dstores: " + formatDstorePorts(selectedDstores));
          System.out.println("Storing file: " + filename + " to Dstores: " + formatDstorePorts(selectedDstores));
          out.println(response);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void handleLoadRequest(Socket clientSocket, String message, PrintWriter out) {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      logger.log(Level.SEVERE, "Malformed LOAD message: " + message);
      return;
    }
    String filename = parts[1];

    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      Index.FileState fileState = index.getFileState(filename);
      if (fileState == null || "IN_PROGRESS".equals(fileState.getStatus())) {
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
            System.out.println("Dstore port not found");
          } else {
            out.println("LOAD_FROM " + port + " " + fileState.getFileSize());
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void handleReloadRequest(Socket clientSocket, String message, PrintWriter out) {
    String[] parts = message.split(" ");
    if (parts.length < 3) {
      logger.log(Level.SEVERE, "Malformed RELOAD message: " + message);
      return;
    }
    String filename = parts[1];
    int failedPort = Integer.parseInt(parts[2]); // Assuming the failed port is sent as part of the command

    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      Index.FileState fileState = index.getFileState(filename);
      if (fileState == null || "IN_PROGRESS".equals(fileState.getStatus())) {
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
              System.out.println("Dstore port not found");
            } else {
              out.println("LOAD_FROM " + port + " " + fileState.getFileSize());
            }
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }


  private void handleStoreAck(String message) {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      logger.log(Level.SEVERE, "Malformed STORE_ACK message: " + message);
      return;
    }
    String filename = parts[1];
    synchronized (this) {
      Integer count = ackCounts.getOrDefault(filename, 0);
      if (count <= 1) {
        ackCounts.remove(filename);
        index.markFileAsComplete(filename);
        storeStartTimes.remove(filename);
        logger.log(Level.INFO, "STORE_COMPLETE for " + filename);
        System.out.println("STORE_COMPLETE for " + filename);

        // Notify the client
        Socket clientSocket = storeRequestClients.remove(filename);
        if (clientSocket != null) {
          try {
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            out.println("STORE_COMPLETE");
          } catch (IOException e) {
            logger.log(Level.SEVERE, "Error sending STORE_COMPLETE to client: " + e.getMessage());
          }
        }
      } else {
        ackCounts.put(filename, count - 1);
      }
    }
  }

  private void handleListRequest(Socket clientSocket, PrintWriter out) {
    if (dstores.size() < replicationFactor) {
      out.println("ERROR_NOT_ENOUGH_DSTORES");
      return;
    }

    List<String> completeFiles = index.getCompleteFiles();
    if (completeFiles.isEmpty()) {
      out.println("LIST");
    } else {
      String fileList = String.join(" ", completeFiles);
      out.println("LIST " + fileList);
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
