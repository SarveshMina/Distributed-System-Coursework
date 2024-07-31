
import java.io.*;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class Controller {
  private int port; // Controller port
  private int r; // Number of Dstores to store a file
  private int timeout; // Timeout for store operation
  private int rebalance_timeout; // Timeout for rebalance operation
  private Index index = new Index(); // Index of files and their file info including status.
  private Map<Socket, Integer> socketIntegerConcurrentHashMap = new ConcurrentHashMap<>();  // Map of Dstore socket to port
  private Map<String, Long> initialStoreTimes = new ConcurrentHashMap<>(); // Map of initial store times for files
  private Map<String, Socket> activeStoreClients = new ConcurrentHashMap<>(); // Map of active store clients
  private Map<String, Socket> activeRemoveClients = new ConcurrentHashMap<>(); // Map of active remove clients
  private Map<String, Integer> acknowledgements = new ConcurrentHashMap<>(); // Map of acknowledgements for files
  private Map<Socket, List<Socket>> clientLoadAttempts = new ConcurrentHashMap<>();   // Map of client load attempts
  private List<Socket> dstoresList = new CopyOnWriteArrayList<>(); // List of Dstores
  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1); // Scheduler for timeouts
  private final Map<String, ReentrantLock> fileLocks = new ConcurrentHashMap<>(); // Locks for files

  /**
   * Constructor for Controller
   *
   * @param port              Controller port number
   * @param r                 Number of Dstores to store a file
   * @param timeout           Timeout for store operation
   * @param rebalance_timeout Timeout for rebalance operation
   */
  public Controller(int port, int r, int timeout, int rebalance_timeout) {
    this.port = port;
    this.r = r;
    this.timeout = timeout;
    this.rebalance_timeout = rebalance_timeout;
    startTimeout();
  }

  /**
   * Start the timeout for STORE and REMOVE operations
   */
  private void startTimeout() {
    scheduler.scheduleAtFixedRate(() -> {
      long currentTime = System.currentTimeMillis();
      initialStoreTimes.forEach((filename, startTime) -> {
        if (currentTime - startTime > timeout && acknowledgements.containsKey(filename) && acknowledgements.get(filename) > 0) {
          synchronized (this) {
            acknowledgements.remove(filename);
            fileLocks.remove(filename);
            index.removeFile(filename);
            System.out.println("Timeout expired for STORE operation of file: " + filename);
          }
        }
      });
    }, 0, 1, TimeUnit.SECONDS);
  }

  /**
   * Schedule timeout for REMOVE operation
   *
   * @param filename file name
   */
  private void removeTimeout(String filename) {
    scheduler.schedule(() -> {
      synchronized (this) {
        if (acknowledgements.containsKey(filename)) {
          acknowledgements.remove(filename);
          fileLocks.remove(filename);
          System.out.println("Timeout expired for REMOVE operation of file: " + filename);
        }
      }
    }, timeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Start the Controller
   * Listen for incoming connections
   */
  public void start() {
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.out.println("Controller started. PORT no: " + port);
      while (true) {
        Socket socket = serverSocket.accept();
        new Thread(() -> handleCommands(socket)).start();
      }
    } catch (IOException e) {
      System.out.println("Error starting Controller: " + e.getMessage());
    }
  }

  /**
   * Handle incoming connection
   * Listen for incoming messages
   * Handle the message
   * Handle JOIN, STORE, STORE_ACK, LOAD, RELOAD, REMOVE, REMOVE_ACK, ERROR_FILE_DOES_NOT_EXIST, LIST Operations
   *
   * @param socket Incoming socket connection
   */
  private void handleCommands(Socket socket) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
      String message;
      while ((message = reader.readLine()) != null) {
        if (message.trim().isEmpty()) {
          System.out.println("Malformed message: " + message + " ignored by Controller");
          continue;
        }

        System.out.println("Received message: " + message);
        String[] msg = message.split(" ");
        try {
          switch (msg[0]) {
            case "JOIN":
              handleJoin(socket, writer, message);
              break;
            case "STORE":
              resetClientLoadAttempts(socket);
              handleStoreCommand(socket, message, writer);
              break;
            case "STORE_ACK":
              handleStoreAck(message);
              break;
            case "LOAD":
              resetClientLoadAttempts(socket);
              handleLoadCommand(socket, message, writer);
              break;
            case "RELOAD":
              handleReloadCommand(socket, message, writer);
              break;
            case "REMOVE":
              resetClientLoadAttempts(socket);
              handleRemoveCommand(socket, message, writer);
              break;
            case "REMOVE_ACK":
              handleRemoveAck(message);
              break;
            case "ERROR_FILE_DOES_NOT_EXIST":
              handleRemoveAck(message);
              break;
            case "LIST":
              resetClientLoadAttempts(socket);
              handleListCommand(writer);
              break;
            default:
              System.out.println("Malformed message: " + message + " ignored by Controller");
              break;
          }
        } catch (Exception e) {
          System.out.println("Error handling message: " + message + " Error: " + e.getMessage());
        }
      }
    } catch (IOException e) {
      System.out.println("Failed to handle connection: " + e.getMessage());
    } finally {
      handleDisconnections(socket);
    }
  }

  /**
   * Handle Dstore disconnection
   * Remove the Dstore from the list of Dstores
   * Remove the Dstore from the index
   *
   * @param socket Disconnected Dstore socket
   */
  private void handleDisconnections(Socket socket) {
    dstoresList.remove(socket);
    Integer dstorePort = socketIntegerConcurrentHashMap.remove(socket);
    if (dstorePort != null) {
      index.removeDstoreFromFiles(socket);
      System.out.println("Dstore on port " + dstorePort + " disconnected and removed.");
    }
  }

  /**
   * Handle JOIN command
   *
   * @param socket  Incoming socket connection
   * @param writer  Writer to write response
   * @param message Incoming message
   */
  private void handleJoin(Socket socket, PrintWriter writer, String message) {
    String[] msg = message.split(" ");
    int dstorePort = Integer.parseInt(msg[1]);
    socketIntegerConcurrentHashMap.put(socket, dstorePort);
    dstoresList.add(socket);
    System.out.println("Dstore joined from: " + dstorePort);
    writer.println("ACK");
  }

  /**
   * Handle LIST command
   *
   * @param writer Writer to write response
   */
  private void handleListCommand(PrintWriter writer) {
    if (dstoresList.size() < r) {
      writer.println("ERROR_NOT_ENOUGH_DSTORES");
      return;
    }

    List<String> completeFiles = index.getCompleteFiles();
    System.out.println(index.getCompleteFiles().toString());
    if (completeFiles.isEmpty()) {
      writer.println("LIST");
    } else {
      String fileList = String.join(" ", completeFiles);
      writer.println("LIST " + fileList);
    }
  }

  /**
   * Handle STORE command.
   * Select Dstores to store the file.
   * Send STORE_TO message to the client.
   *
   * @param clientSocket client socket connection to send response
   * @param msg          incoming message
   * @param writer       writer to write response
   */
  private void handleStoreCommand(Socket clientSocket, String msg, PrintWriter writer) {
    String[] msgParts = msg.split(" ");
    if (msgParts.length != 3) {
      return;
    }
    String filename = msgParts[1];
    int fileSize = Integer.parseInt(msgParts[2]);

    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      if (index.isFileComplete(filename) || index.isFileInProgress(filename)) {
        System.out.println("File already exists or is in progress");
        writer.println("ERROR_FILE_ALREADY_EXISTS");
      } else if (dstoresList.size() < r) {
        System.out.println("Not enough Dstores");
        writer.println("ERROR_NOT_ENOUGH_DSTORES");
      } else {
        List<Socket> selectedDstores = randomDstore();
        if (selectedDstores.size() < r) {
          writer.println("ERROR_NOT_ENOUGH_DSTORES");
        } else {
          index.fileToStore(filename, selectedDstores, fileSize);
          System.out.println(index.getInProgressFilesInfo());

          acknowledgements.put(filename, r);
          initialStoreTimes.put(filename, System.currentTimeMillis());
          activeStoreClients.put(filename, clientSocket);  // Track client socket
          String response = "STORE_TO " + formatDstorePort(selectedDstores);
          System.out.println("Storing file: " + filename + " to Dstores: " + formatDstorePort(selectedDstores));
          writer.println(response);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Handle STORE_ACK message
   * STORE_COMPLETE if all Dstores have acknowledged
   *
   * @param msg incoming message from Dstores.
   */
  private void handleStoreAck(String msg) {
    String[] parts = msg.split(" ");
    if (parts.length < 2) {
      return;
    }

    String filename = parts[1];

    synchronized (this) {
      Integer count = acknowledgements.getOrDefault(filename, 0);
      if (count <= 1) {
        acknowledgements.remove(filename);
        index.markFileAsComplete(filename);
        initialStoreTimes.remove(filename);
        System.out.println("STORE Success for " + filename);

        Socket clientSocket = activeStoreClients.remove(filename);
        if (clientSocket != null) {
          try {
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
            writer.println("STORE_COMPLETE");
          } catch (IOException e) {
            System.out.println("Error can't send STORE_COMPLETE to client: " + e.getMessage());
          }
        }
      } else {
        acknowledgements.put(filename, count - 1);
      }
    }
  }

  /**
   * Handle LOAD command
   * Select Dstore to load the file
   * Send LOAD_FROM message to the client
   *
   * @param clientSocket client socket connection to send response
   * @param msg          incoming message
   * @param writer       writer to write response
   */
  private void handleLoadCommand(Socket clientSocket, String msg, PrintWriter writer) {
    String[] parts = msg.split(" ");
    if (parts.length != 2) {
      return;
    }
    String filename = parts[1];

    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      if (dstoresList.size() < r) {
        writer.println("ERROR_NOT_ENOUGH_DSTORES");
      } else if (index.getCompleteFileInfo(filename) == null || index.getInProgressFilesInfo().containsKey(filename)) {
        writer.println("ERROR_FILE_DOES_NOT_EXIST");
      } else {
        List<Socket> dstoreSockets = index.getCompleteFileInfo(filename).getDstoreSockets();
        clientLoadAttempts.put(clientSocket, new ArrayList<>(dstoreSockets)); // The original list of Dstores.
        Socket selectedDstore = dstoresForLoadOperation(clientSocket);
        if (selectedDstore == null) {
          writer.println("ERROR_LOAD");
        } else {
          Integer port = socketIntegerConcurrentHashMap.get(selectedDstore);
          if (port == null) {
            System.out.println("Port not found");
          } else {
            writer.println("LOAD_FROM " + port + " " + index.getCompleteFileInfo(filename).getFileSize());
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Handle RELOAD command
   * Select Dstore to load the file
   *
   * @param clientSocket client socket connection to send response
   * @param msg          incoming message
   * @param writer       writer to write response
   */
  private void handleReloadCommand(Socket clientSocket, String msg, PrintWriter writer) {
    String[] parts = msg.split(" ");
    if (parts.length < 2) {
      return;
    }
    String filename = parts[1];

    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      List<Socket> dstoreSockets = clientLoadAttempts.get(clientSocket);
      if (dstoreSockets == null || dstoreSockets.isEmpty()) {
        writer.println("ERROR_LOAD");
        return;
      }

      Socket selectedDstore = dstoresForLoadOperation(clientSocket);
      if (selectedDstore == null) {
        writer.println("ERROR_LOAD");
      } else {
        Integer port = socketIntegerConcurrentHashMap.get(selectedDstore);
        if (port == null) {
          System.out.println("Dstore port not found");
        } else {
          writer.println("LOAD_FROM " + port + " " + index.getCompleteFileInfo(filename).getFileSize());
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Handle REMOVE command
   * Select Dstores to remove the file
   * Send REMOVE message to Dstores
   *
   * @param clientSocket client socket connection to send response
   * @param message      incoming message
   * @param out          writer to write response
   */
  private void handleRemoveCommand(Socket clientSocket, String message, PrintWriter out) {
    String[] parts = message.split(" ");
    if (parts.length != 2) {
      System.out.println("Malformed message: " + message);
      return;
    }
    String filename = parts[1];

    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {

      if (dstoresList.size() < r) {
        out.println("ERROR_NOT_ENOUGH_DSTORES");
      } else if (!index.isFileComplete(filename) || index.getInProgressFilesInfo().containsKey(filename)) {
        out.println("ERROR_FILE_DOES_NOT_EXIST");
      } else {
        List<Socket> dstoreSockets = new ArrayList<>(index.getCompleteFileInfo(filename).getDstoreSockets());
        acknowledgements.put(filename, dstoreSockets.size());
        activeRemoveClients.put(filename, clientSocket);
        index.markFileAsRemoveInProgress(filename);
        for (Socket dstore : dstoreSockets) {
          Integer port = socketIntegerConcurrentHashMap.get(dstore);
          if (port != null) {
            try {
              PrintWriter dstoreOut = new PrintWriter(dstore.getOutputStream(), true);
              dstoreOut.println("REMOVE " + filename);
            } catch (IOException e) {
              System.out.println("Error sending REMOVE reply to Dstore: " + e.getMessage());
            }
          }
        }
        removeTimeout(filename);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Handle REMOVE_ACK message
   * Remove the file if all Dstores have acknowledged
   *
   * @param message incoming message from Dstores
   */
  private void handleRemoveAck(String message) {
    String[] parts = message.split(" ");
    if (parts.length != 2) {
      return;
    }
    String filename = parts[1];
    synchronized (this) {
      Integer count = acknowledgements.getOrDefault(filename, 0);
      if (count <= 1) {
        acknowledgements.remove(filename);
        index.removeFile(filename);
        fileLocks.remove(filename);
        System.out.println("REMOVE_COMPLETE for " + filename);

        // Notify the client
        Socket clientSocket = activeRemoveClients.remove(filename);
        if (clientSocket != null) {
          try {
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            out.println("REMOVE_COMPLETE");
          } catch (IOException e) {
            System.out.println("Error sending REMOVE_COMPLETE reply to client: " + e.getMessage());
          }
        }
      } else {
        acknowledgements.put(filename, count - 1);
      }
    }
  }

  /**
   * Select Dstore for LOAD operation
   *
   * @param clientSocket client socket connection
   * @return selected Dstore
   */
  private Socket dstoresForLoadOperation(Socket clientSocket) {
    List<Socket> dstoreSockets = clientLoadAttempts.get(clientSocket);
    if (dstoreSockets == null || dstoreSockets.isEmpty()) return null;

    return dstoreSockets.remove(new Random().nextInt(dstoreSockets.size()));
  }

  /**
   * Reset client load attempts
   *
   * @param clientSocket client socket connection
   */
  private void resetClientLoadAttempts(Socket clientSocket) {
    clientLoadAttempts.remove(clientSocket);
  }

  /**
   * Select Dstores to store the file
   *
   * @return list of selected Dstores
   */
  private List<Socket> randomDstore() {
    List<Socket> selected = new ArrayList<>(dstoresList);
    Collections.shuffle(selected);
    return selected.subList(0, Math.min(r, selected.size()));
  }

  /**
   * Format Dstore port numbers
   *
   * @param dstores list of Dstores
   * @return formatted Dstore port numbers
   */
  private String formatDstorePort(List<Socket> dstores) {
    StringBuilder sb = new StringBuilder();
    for (Socket dstore : dstores) {
      Integer port = socketIntegerConcurrentHashMap.get(dstore);
      if (port != null) {
        sb.append(port).append(" ");
      }
    }
    return sb.toString().trim();
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Need to provide 4 arguments: <cport> <R> <timeout> <rebalance_period>");
      return;
    }
    int cport = Integer.parseInt(args[0]);
    int R = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    int rebalance_period = Integer.parseInt(args[3]);
    new Controller(cport, R, timeout, rebalance_period).start();
  }
}