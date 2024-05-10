import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Dstore {
  private int port;
  private int controllerPort;
  private File fileFolder;
  private Socket controllerSocket;
  private PrintWriter controllerOut;

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
      // Create streams for reading command and writing responses
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

      // Read the command
      String command = in.readLine();
      System.out.println("Received command: " + command);

      if (command != null && command.startsWith("STORE")) {
        String[] parts = command.split(" ");
        String filename = parts[1];
        int fileSize = Integer.valueOf(parts[2]);
        byte[] buffer = new byte[fileSize];
        int blufen;

        File file = new File(fileFolder, filename);
        FileOutputStream fileOut = new FileOutputStream(file);
        InputStream rawInput = clientSocket.getInputStream();
        System.out.println("filename " + filename + " fileSize " + fileSize);
        out.println("ACK");

        while ((blufen = rawInput.read(buffer)) != -1) {
          fileOut.write(buffer, 0, blufen);
        }

        controllerOut.println("STORE_ACK " + filename);
        System.out.println("Stored file: " + filename + " and sent ACK.");
      }
    } catch (IOException e) {
      System.out.println("Error handling client request: " + e.getMessage());
      e.printStackTrace();
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
