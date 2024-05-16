import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Test1Client {
  public static void main(String[] args) throws Exception {
    final int cport = 12345;
    final int r = 3;
    final int n = 3;
    final int dport1 = 12346;
    final int dport2 = 12347;
    final int dport3 = 12348;
    final int timeout = 1000;
    final int rebalance_period = 100000;

    setupFiles();

    Client client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

    try {
      client.connect();

      // list - expect empty list
      printList(client);

      // store small_file
      File smallFile = new File("to_store/small_file");
      client.store(smallFile);

      // store again small_file - expect file already exists
      try {
        client.store(smallFile);
      } catch (FileAlreadyExistsException e) {
        System.out.println("File already exists: small_file");
      }

      // list - expect small_file
      printList(client);

      // load small_file
      client.load("small_file", new File("downloads"));

      // (wrong) load small_file
      try {
        client.wrongLoad("small_file", 1);
      } catch (IOException e) {
        System.out.println("Error during wrong load operation: " + e.getMessage());
      }

      // remove small_file
      client.remove("small_file");

      // list - expect empty list
      printList(client);

      // (wrong) store small_file
      try {
        client.wrongStore("small_file", null);
      } catch (IOException e) {
        System.out.println("Error during wrong store operation: " + e.getMessage());
      }

      // load another_file - expect file does not exist
      try {
        client.load("another_file", new File("downloads"));
      } catch (FileDoesNotExistException e) {
        System.out.println("File does not exist: another_file");
      }

      // remove another_file - expect file does not exist
      try {
        client.remove("another_file");
      } catch (FileDoesNotExistException e) {
        System.out.println("File does not exist: another_file");
      }
    } finally {
      if (client != null) {
        try {
          client.disconnect();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void printList(Client client) throws IOException, NotEnoughDstoresException {
    String[] list = client.list();
    System.out.println("List of files:");
    for (String file : list) {
      System.out.println(file);
    }
  }

  private static void setupFiles() throws IOException {
    // Create directories if they do not exist
    Files.createDirectories(Path.of("to_store"));
    Files.createDirectories(Path.of("downloads"));

    // Create a small file for testing
    File smallFile = new File("to_store/small_file");
    if (!smallFile.exists()) {
      Files.writeString(smallFile.toPath(), "small content");
    }
  }
}
