import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class Dstore {
  private int port;
  private int controllerPort;
  private File fileFolder;
  private Socket controllerSocket;
  private PrintWriter controllerOut;
  private BufferedReader controllerIn;
  private static final Logger logger = Logger.getLogger(Dstore.class.getName());
  private int timeout;
  private final ConcurrentHashMap<String, ReentrantLock> fileLocks = new ConcurrentHashMap<>();

  public Dstore(int port, int controllerPort, int timeout, String fileFolderPath) {
    this.port = port;
    this.controllerPort = controllerPort;
    this.timeout = timeout;
    this.fileFolder = new File(fileFolderPath);
    if (!fileFolder.exists()) {
      fileFolder.mkdirs();
    }
  }

  public void start() {
    connectToController();
    new Thread(this::handleControllerCommands).start();
    handleClientConnections();
  }

  private void connectToController() {
    while (true) {
      try {
        controllerSocket = new Socket("localhost", controllerPort);
        controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
        controllerIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
        controllerOut.println("JOIN " + port);
        String response = controllerIn.readLine();
        if ("ACK".equals(response)) {
          System.out.println("Joined Controller successfully on port " + port);
          break;
        }
      } catch (IOException e) {
        System.out.println("Failed to connect to Controller: " + e.getMessage());
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private void handleControllerCommands() {
    try {
      String command;
      while ((command = controllerIn.readLine()) != null) {
        System.out.println("Received command from Controller: " + command);
        handleCommand(command);
      }
    } catch (IOException e) {
      System.out.println("Connection to Controller lost: " + e.getMessage());
    }
  }

  private void handleCommand(String command) {
    String[] parts = command.split(" ");
    if (parts.length < 2) {
      return;
    }
    switch (parts[0]) {
      case "REMOVE":
        handleRemove(parts[1]);
        break;
    }
  }

  private void handleRemove(String filename) {
    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      File file = new File(fileFolder, filename);
      if (file.delete()) {
        controllerOut.println("REMOVE_ACK " + filename);
        System.out.println("File " + filename + " removed successfully.");
      } else {
        System.out.println("Failed to remove file: " + filename);
      }
    } finally {
      lock.unlock();
      fileLocks.remove(filename);
    }
  }

  private void handleClientConnections() {
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.out.println("Dstore listening on port " + port);
      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start();
      }
    } catch (IOException e) {
      System.out.println("Error starting Dstore on port " + port + ": " + e.getMessage());
    }
  }

  private void handleClient(Socket clientSocket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
      String command;
      while ((command = in.readLine()) != null) {
        System.out.println("Received command from client: " + command);
        String[] parts = command.split(" ");
        if (parts.length < 2) {
          out.println("ERROR Invalid command");
          continue;
        }
        switch (parts[0]) {
          case "STORE":
            handleStore(parts[1], Integer.parseInt(parts[2]), clientSocket, out);
            break;
          case "LOAD_DATA":
            handleLoadData(parts[1], clientSocket);
            break;
          default:
            out.println("ERROR Invalid command");
        }
      }
    } catch (IOException e) {
      System.out.println("Error handling client request: " + e.getMessage());
    } finally {
      try {
        clientSocket.close(); // Ensure the socket is closed after handling the client
      } catch (IOException e) {
        System.out.println("Failed to close client socket: " + e.getMessage());
      }
    }
  }

  private void handleStore(String filename, int fileSize, Socket clientSocket, PrintWriter out) {
    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      File file = new File(fileFolder, filename);
      byte[] buffer = new byte[fileSize];  // Use a buffer the size of the expected file

      try (FileOutputStream fileOut = new FileOutputStream(file);
           InputStream rawInput = clientSocket.getInputStream()) {

        out.println("ACK");  // Acknowledge that Dstore is ready to receive the file

        int totalBytesRead = 0;
        int bytesRead;
        while (totalBytesRead < fileSize && (bytesRead = rawInput.read(buffer, totalBytesRead, fileSize - totalBytesRead)) != -1) {
          fileOut.write(buffer, 0, bytesRead);
          totalBytesRead += bytesRead;
        }

        if (totalBytesRead < fileSize) {
          throw new IOException("Did not receive the full file");
        }

        controllerOut.println("STORE_ACK " + filename);
        System.out.println("Stored file: " + filename + " and sent ACK.");
      } catch (IOException e) {
        System.out.println("Error storing file: " + filename + ": " + e.getMessage());
        if (file.exists()) {
          file.delete();  // Attempt to delete the file if an error occurs during storage
        }
      }
    } finally {
      lock.unlock();
      fileLocks.remove(filename);
    }
  }

  private void handleLoadData(String filename, Socket clientSocket) throws IOException {
    ReentrantLock lock = fileLocks.computeIfAbsent(filename, k -> new ReentrantLock());
    lock.lock();
    try {
      File file = new File(fileFolder, filename);
      if (!file.exists()) {
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        out.println("ERROR_FILE_DOES_NOT_EXIST");
        clientSocket.close();  // Close the connection if the file does not exist
        return;
      }

      try (InputStream fileInput = new FileInputStream(file);
           OutputStream clientOutput = clientSocket.getOutputStream()) {
        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = fileInput.read(buffer)) != -1) {
          clientOutput.write(buffer, 0, bytesRead);
        }
        clientOutput.flush();
        System.out.println("File " + filename + " sent to client.");
      } catch (IOException e) {
        System.out.println("Failed to send file " + filename + ": " + e.getMessage());
        throw e;
      }
    } finally {
      lock.unlock();
      fileLocks.remove(filename);
    }
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: java Dstore <port> <controllerPort> <fileFolder> <timeout>");
      return;
    }
    int port = Integer.parseInt(args[0]);
    int controllerPort = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    String fileFolder = args[3];
    new Dstore(port, controllerPort, timeout, fileFolder).start();
  }
}
