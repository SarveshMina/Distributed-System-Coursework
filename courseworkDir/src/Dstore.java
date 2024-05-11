import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
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

  public Dstore(int port, int controllerPort, String fileFolderPath) {
    this.port = port;
    this.controllerPort = controllerPort;
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
          handleLoadData(clientSocket, command, out);
        }
      }
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

  private void handleLoadData(Socket clientSocket, String message, PrintWriter out) {
    String[] parts = message.split(" ");
    if (parts.length < 2) {
      out.println("ERROR_MALFORMED_COMMAND");
      return;
    }
    String filename = parts[1];
    File file = new File(fileFolder, filename);
    if (!file.exists()) {
      out.println("ERROR_FILE_DOES_NOT_EXIST");
      return;
    }
    try (InputStream fileInput = new FileInputStream(file)) {
      byte[] buffer = new byte[4096];
      int bytesRead;
      while ((bytesRead = fileInput.read(buffer)) != -1) {
        clientSocket.getOutputStream().write(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      System.out.println("Failed to send file " + filename + ": " + e.getMessage());
    }
  }


  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: java Dstore <port> <controllerPort> <fileFolder>");
      return;
    }
    int port = Integer.parseInt(args[0]);
    int controllerPort = Integer.parseInt(args[1]);
    String fileFolder = args[2];
    new Dstore(port, controllerPort, fileFolder).start();
  }
}
