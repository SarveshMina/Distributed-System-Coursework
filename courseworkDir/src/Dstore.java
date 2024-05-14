import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Dstore {
  private int port;
  private int controllerPort;
  private File fileFolder;
  private Socket controllerSocket;
  private PrintWriter controllerOut;
  private static final Logger logger = Logger.getLogger(Controller.class.getName());
  private int timeout;

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
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.out.println("Dstore listening on port " + port);
      while (true) {
        Socket clientSocket = serverSocket.accept();
        clientSocket.setSoTimeout(timeout * 1000);  // Set timeout for the client socket
        new Thread(() -> handleClient(clientSocket)).start();
      }
    } catch (IOException e) {
      System.out.println("Error starting Dstore on port " + port + ": " + e.getMessage());
    }
  }

  private void connectToController() {
    try {
      controllerSocket = new Socket("localhost", controllerPort);
      controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
      controllerOut.println("JOIN " + port);
      BufferedReader in = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
      if ("ACK".equals(in.readLine())) {
        System.out.println("Joined Controller successfully on port " + port);
      }
    } catch (IOException e) {
      System.out.println("Failed to connect to Controller: " + e.getMessage());
    }
  }

  private void handleClient(Socket clientSocket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
      String command = in.readLine();
      System.out.println("Received command: " + command);

      if (command != null) {
        if (command.startsWith("STORE")) {
          handleStore(clientSocket, command);
        } else if (command.startsWith("LOAD_DATA")) {
          //TODO: Implement LOAD_DATA
        }
      }
    } catch (SocketTimeoutException e) {
      System.out.println("Timeout occurred while handling client request: " + e.getMessage());
    } catch (IOException e) {
      System.out.println("Error handling client request: " + e.getMessage());
    }
  }

  private void handleStore(Socket clientSocket, String command) throws IOException {
    String[] parts = command.split(" ");
    String filename = parts[1];
    int fileSize = Integer.parseInt(parts[2]);
    byte[] buffer = new byte[fileSize];

    File file = new File(fileFolder, filename);
    try (FileOutputStream fileOut = new FileOutputStream(file);
         InputStream rawInput = clientSocket.getInputStream();
         PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

      out.println("ACK");
      int bytesRead;
      while ((bytesRead = rawInput.read(buffer)) != -1) {
        fileOut.write(buffer, 0, bytesRead);
      }

      controllerOut.println("STORE_ACK " + filename);
      System.out.println("Stored file: " + filename + " and sent ACK.");
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
