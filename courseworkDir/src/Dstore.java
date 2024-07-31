import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Dstore {
  private int port; // Port number for the Dstore
  private int cport; // Port number for the Controller
  private File storageDir; // Directory to store files
  private Socket ctrlSocket; // Socket connection to the Controller
  private PrintWriter ctrlWriter; // PrintWriter to send messages to the Controller
  private BufferedReader ctrlIn; // BufferedReader to read messages from the Controller
  private int timeout; // Timeout for the Dstore
  private final ConcurrentHashMap<String, ReentrantLock> fileLocks = new ConcurrentHashMap<>(); // Locks for file access
  private static final Logger logger = Logger.getLogger(Dstore.class.getName()); // Logger for the Dstore

  /**
   * Constructor for the Dstore class.
   *
   * @param port        The port number for the Dstore.
   * @param cport       The port number for the Controller.
   * @param timeout     The timeout for the Dstore.
   * @param storagePath The path to the storage directory.
   */
  public Dstore(int port, int cport, int timeout, String storagePath) {
    this.port = port;
    this.cport = cport;
    this.timeout = timeout;
    this.storageDir = new File(storagePath);
    if (!storageDir.exists()) {
      storageDir.mkdirs();
    }
    configureLogger();
  }

  /**
   * Configure the logger to create a unique log file for each Dstore instance.
   */
  private void configureLogger() {
    try {
      int counter = 0;
      File logFile;
      do {
        logFile = new File(storageDir, "dstore-" + port + (counter == 0 ? "" : "-" + counter) + ".log");
        counter++;
      } while (logFile.exists());

      FileHandler fileHandler = new FileHandler(logFile.getAbsolutePath(), false);
      fileHandler.setFormatter(new SimpleFormatter());
      logger.addHandler(fileHandler);
      logger.setUseParentHandlers(false);
    } catch (IOException e) {
      System.err.println("Failed to configure logger: " + e.getMessage());
    }
  }

  /**
   * Start the Dstore by connecting to the controller and handling client requests.
   */
  public void run() {
    connectToController();
    new Thread(this::handleControllerCommands).start();
    handleClientRequests();
  }

  /**
   * Connect to the controller and join the network.
   */
  private void connectToController() {
    while (true) {
      try {
        ctrlSocket = new Socket("localhost", cport);
        ctrlWriter = new PrintWriter(ctrlSocket.getOutputStream(), true);
        ctrlIn = new BufferedReader(new InputStreamReader(ctrlSocket.getInputStream()));
        ctrlWriter.println("JOIN " + port);
        String response = ctrlIn.readLine();
        if ("ACK".equals(response)) {
          System.out.println("Connected to Controller on port " + port);
          logger.info("Connected to Controller on port " + port);
          break;
        }
      } catch (IOException e) {
        System.out.println("Connection to Controller failed: " + e.getMessage());
        logger.severe("Connection to Controller failed: " + e.getMessage());
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Handle commands received from the controller.
   */
  private void handleControllerCommands() {
    try {
      String command;
      while ((command = ctrlIn.readLine()) != null) {
        System.out.println("Received command from Controller: " + command);
        logger.info("Received command from Controller: " + command);
        processCommand(command);
      }
    } catch (IOException e) {
      System.out.println("Lost connection to Controller: " + e.getMessage());
      logger.severe("Lost connection to Controller: " + e.getMessage());
    }
  }

  /**
   * Process individual commands from the controller.
   *
   * @param command The command to process.
   */
  private void processCommand(String command) {
    String[] cmdParts = command.split(" ");
    if (cmdParts.length < 2) {
      return;
    }
    switch (cmdParts[0]) {
      case "REMOVE":
        processRemove(cmdParts[1]);
        break;
    }
  }

  /**
   * Process the REMOVE command from the controller.
   *
   * @param filename The name of the file to remove.
   */
  private void processRemove(String filename) {
    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      File file = new File(storageDir, filename);
      if (!file.exists()) {
        ctrlWriter.println("ERROR_FILE_DOES_NOT_EXIST " + filename);
        System.out.println("File " + filename + " does not exist.");
        logger.info("File " + filename + " does not exist.");
      } else if (file.delete()) {
        ctrlWriter.println("REMOVE_ACK " + filename);
        System.out.println("File " + filename + " removed successfully.");
        logger.info("File " + filename + " removed successfully.");
      } else {
        System.out.println("Failed to remove file: " + filename);
        logger.warning("Failed to remove file: " + filename);
      }
    } finally {
      lock.unlock();
      fileLocks.remove(filename);
    }
  }

  /**
   * Handle incoming client requests.
   */
  private void handleClientRequests() {
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.out.println("Dstore listening on port " + port);
      logger.info("Dstore listening on port " + port);
      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start();
      }
    } catch (IOException e) {
      System.out.println("Error starting Dstore on port " + port + ": " + e.getMessage());
      logger.severe("Error starting Dstore on port " + port + ": " + e.getMessage());
    }
  }

  /**
   * Handle individual client requests.
   *
   * @param clientSocket The client socket connection.
   */
  private void handleClient(Socket clientSocket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
      String command;
      while ((command = in.readLine()) != null) {
        System.out.println("Received command from client: " + command);
        logger.info("Received command from client: " + command);
        String[] cmdParts = command.split(" ");
        if (cmdParts.length < 2) {
          out.println("ERROR Invalid command");
          continue;
        }
        switch (cmdParts[0]) {
          case "STORE":
            processStore(cmdParts[1], Integer.parseInt(cmdParts[2]), clientSocket, out);
            break;
          case "LOAD_DATA":
            processLoadData(cmdParts[1], clientSocket);
            break;
          default:
            out.println("ERROR Invalid command");
        }
      }
    } catch (IOException e) {
      System.out.println("Error handling client request: " + e.getMessage());
      logger.severe("Error handling client request: " + e.getMessage());
    } finally {
      try {
        clientSocket.close(); // To ensure the socket is closed after handling the client
      } catch (IOException e) {
        System.out.println("Failed to close client socket: " + e.getMessage());
        logger.severe("Failed to close client socket: " + e.getMessage());
      }
    }
  }

  /**
   * Process the STORE command from a client.
   *
   * @param filename     The name of the file to store.
   * @param fileSize     The size of the file to store.
   * @param clientSocket The client socket connection.
   * @param out          The PrintWriter to send responses to the client.
   */
  private void processStore(String filename, int fileSize, Socket clientSocket, PrintWriter out) {
    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      File file = new File(storageDir, filename);
      byte[] buffer = new byte[fileSize];  // Use a buffer the size of the expected file

      try (FileOutputStream fileOut = new FileOutputStream(file); InputStream rawInput = clientSocket.getInputStream()) {

        out.println("ACK");  // Acknowledge that DataStore is ready to receive the file

        int totalBytesRead = 0;
        int bytesRead;
        while (totalBytesRead < fileSize && (bytesRead = rawInput.read(buffer, totalBytesRead, fileSize - totalBytesRead)) != -1) {
          fileOut.write(buffer, 0, bytesRead);
          totalBytesRead += bytesRead;
        }

        if (totalBytesRead < fileSize) {
          throw new IOException("Did not receive the full file");
        }

        ctrlWriter.println("STORE_ACK " + filename);
        System.out.println("Stored file: " + filename + " and sent ACK.");
        logger.info("Stored file: " + filename + " and sent ACK.");
      } catch (IOException e) {
        System.out.println("Error storing file: " + filename + ": " + e.getMessage());
        logger.severe("Error storing file: " + filename + ": " + e.getMessage());
        if (file.exists()) {
          file.delete();  // Attempt to delete the file if an error occurs during storage
        }
      }
    } finally {
      lock.unlock();
      fileLocks.remove(filename);
    }
  }

  /**
   * Process the LOAD_DATA command from a client.
   *
   * @param filename     The name of the file to load.
   * @param clientSocket The client socket connection.
   * @throws IOException If an I/O error occurs.
   */
  private void processLoadData(String filename, Socket clientSocket) throws IOException {
    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      File file = new File(storageDir, filename);
      if (!file.exists()) {
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        out.println("ERROR_FILE_DOES_NOT_EXIST");
        clientSocket.close();  // Close the connection if the file does not exist
        return;
      }

      try (InputStream fileInput = new FileInputStream(file); OutputStream clientOutput = clientSocket.getOutputStream()) {
        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = fileInput.read(buffer)) != -1) {
          clientOutput.write(buffer, 0, bytesRead);
        }
        clientOutput.flush();
        System.out.println("File " + filename + " sent to client.");
        logger.info("File " + filename + " sent to client.");
      } catch (IOException e) {
        System.out.println("Failed to send file " + filename + ": " + e.getMessage());
        logger.severe("Failed to send file " + filename + ": " + e.getMessage());
        throw e;
      }
    } finally {
      lock.unlock();
      fileLocks.remove(filename);
    }
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Need to provide 4 arguments: <dstorePort> <ctrlPort> <timeoutMs> <storageDir>");
      return;
    }
    int dstorePort = Integer.parseInt(args[0]);
    int ctrlPort = Integer.parseInt(args[1]);
    int timeoutMs = Integer.parseInt(args[2]);
    String storageDir = args[3];
    new Dstore(dstorePort, ctrlPort, timeoutMs, storageDir).run();
  }
}