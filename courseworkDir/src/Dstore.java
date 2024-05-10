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
      fileFolder.mkdirs(); // Ensure the directory exists
    }
  }

  public void start() {
    connectToController();
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.out.println("Dstore listening on port " + port);
      while (true) {
        Socket clientSocket = serverSocket.accept();
        // Client handling logic could be added here
      }
    } catch (IOException e) {
      System.out.println("Error starting Dstore on port " + port + ": " + e.getMessage());
    }
  }

  private void connectToController() {
    try {
      controllerSocket = new Socket("localhost", controllerPort);
      controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
      BufferedReader in = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

      // Send the Dstore's listening port along with the JOIN message
      controllerOut.println("JOIN " + port);

      // Read acknowledgement from the controller
      String ack = in.readLine();
      if ("ACK".equals(ack)) {
        System.out.println("Joined Controller successfully on port " + port);
        // Keep the socket open for further commands or till the application ends
        // Consider adding a loop here to listen to commands from the controller
      } else {
        System.out.println("Failed to join controller, no ACK received.");
      }
    } catch (IOException e) {
      System.out.println("Failed to connect to Controller: " + e.getMessage());
      // Consider a reconnect strategy here or close resources properly
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
