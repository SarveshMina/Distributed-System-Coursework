import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestStoreClient {

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: java TestStoreClient <controllerPort> <timeout> <directory>");
      return;
    }

    int controllerPort = Integer.parseInt(args[0]);
    int timeout = Integer.parseInt(args[1]);
    String directoryPath = args[2];

    File directory = new File(directoryPath);
    if (!directory.exists() || !directory.isDirectory()) {
      System.out.println("Directory does not exist or is not a directory: " + directoryPath);
      return;
    }

    // Create a subdirectory for loaded files
    File loadedFilesDirectory = new File("loadedFiles");
    if (!loadedFilesDirectory.exists()) {
      loadedFilesDirectory.mkdirs();  // Create the directory if it doesn't exist
    }

    File[] files = directory.listFiles(); // List all files, regardless of extension
    if (files == null || files.length == 0) {
      System.out.println("No files to store in the directory.");
      return;
    }

    Client client = new Client(controllerPort, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
    try {
      client.connect();
      // Storing files
      for (File file : files) {
        try {
          System.out.println("Attempting to store file: " + file.getName());
          client.store(file);
          System.out.println("File stored successfully: " + file.getName());
        } catch (Exception e) {
          System.err.println("Error storing file " + file.getName() + ": " + e.getMessage());
          e.printStackTrace();
        }
      }

      // Loading the first file to check if load functionality works
      if (files.length > 0) {
        String filenameToLoad = files[0].getName();
        System.out.println("Attempting to load file: " + filenameToLoad);
        byte[] data = client.load(filenameToLoad);
        System.out.println("File loaded successfully: " + filenameToLoad + ", size: " + data.length + " bytes");

        // Save the loaded data to the loadedFiles directory
        File outputFile = new File(loadedFilesDirectory, "loaded_" + filenameToLoad);
        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
          fos.write(data);
          System.out.println("Loaded data written to: " + outputFile.getAbsolutePath());
        }
      }
    } catch (IOException e) {
      System.err.println("Failed to connect or error during file storage/load: " + e.getMessage());
      e.printStackTrace();
    } finally {
      try {
        client.disconnect();
      } catch (IOException e) {
        System.err.println("Error disconnecting client: " + e.getMessage());
      }
    }
  }
}
