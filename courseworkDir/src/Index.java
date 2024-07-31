import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Index class manages the state of files within the system.
 * It tracks files that are in progress, complete, or marked for removal.
 */
public class Index {
  public final ConcurrentHashMap<String, FileInfo> inProgressFilesInfo = new ConcurrentHashMap<>();  //Stores information about files that are in progress.
  public final ConcurrentHashMap<String, FileInfo> completeFilesInfo = new ConcurrentHashMap<>();   //Stores information about files that have been completely stored.
  public final ConcurrentHashMap<String, FileInfo> removeFilesInfo = new ConcurrentHashMap<>();    //Stores information about files that are marked for removal.

  /**
   * Returns the map containing information about files that are in progress.
   *
   * @return ConcurrentHashMap of in-progress files
   */
  public ConcurrentHashMap<String, FileInfo> getInProgressFilesInfo() {
    return inProgressFilesInfo;
  }

  /**
   * Adds a file to the in-progress list and initializes its storage information.
   *
   * @param filename the name of the file
   * @param dstores  the list of dstores where the file is being stored
   * @param fileSize the size of the file
   */
  public synchronized void fileToStore(String filename, List<Socket> dstores, int fileSize) {
    inProgressFilesInfo.putIfAbsent(filename, new FileInfo(dstores, fileSize, "IN_PROGRESS"));
    System.out.println(inProgressFilesInfo.get(filename).getDstoreSockets() + " is storing " + inProgressFilesInfo.get(filename).getFileSize() + " of size " + inProgressFilesInfo.get(filename).getFileSize() + " bytes");
  }

  /**
   * Checks if a file is currently in progress.
   *
   * @param filename the name of the file
   * @return true if the file is in progress, false otherwise
   */
  public synchronized boolean isFileInProgress(String filename) {
    FileInfo fileInfo = inProgressFilesInfo.get(filename);
    return fileInfo != null && "IN_PROGRESS".equals(fileInfo.statusInfo);
  }

  /**
   * Checks if a file has been completely stored.
   *
   * @param filename the name of the file
   * @return true if the file is complete, false otherwise
   */
  public synchronized boolean isFileComplete(String filename) {
    return completeFilesInfo.containsKey(filename);
  }

  /**
   * Marks a file as complete by moving it from the in-progress list to the complete list.
   *
   * @param filename the name of the file
   */
  public synchronized void markFileAsComplete(String filename) {
    if (inProgressFilesInfo.get(filename) != null) {
      inProgressFilesInfo.get(filename).setStatus("COMPLETE");
      completeFilesInfo.put(filename, inProgressFilesInfo.get(filename));
      inProgressFilesInfo.remove(filename);
    }
  }

  /**
   * Marks a file as "remove in progress" by moving it from the complete list to the remove list.
   *
   * @param filename the name of the file
   */
  public synchronized void markFileAsRemoveInProgress(String filename) {
    if (completeFilesInfo.get(filename) != null) {
      completeFilesInfo.get(filename).setStatus("REMOVE_IN_PROGRESS");
      removeFilesInfo.put(filename, completeFilesInfo.get(filename));
      completeFilesInfo.remove(filename);
    }
  }

  /**
   * Removes a file from all lists (in-progress, complete, and remove).
   *
   * @param filename the name of the file
   */
  public synchronized void removeFile(String filename) {
    inProgressFilesInfo.remove(filename);
    completeFilesInfo.remove(filename);
    removeFilesInfo.remove(filename);
  }

  /**
   * Retrieves information about a completely stored file.
   *
   * @param filename the name of the file
   * @return FileInfo object containing information about the file
   */
  public FileInfo getCompleteFileInfo(String filename) {
    return completeFilesInfo.get(filename);
  }

  /**
   * Retrieves a list of all complete files.
   *
   * @return List of complete file names
   */
  public synchronized List<String> getCompleteFiles() {
    return new ArrayList<>(completeFilesInfo.keySet());
  }

  /**
   * Removes a Dstore from all file records.
   *
   * @param dstore the socket of the Dstore to be removed
   */
  public synchronized void removeDstoreFromFiles(Socket dstore) {
    inProgressFilesInfo.values().forEach(fileInfo -> fileInfo.getDstoreSockets().remove(dstore));
    completeFilesInfo.values().forEach(fileInfo -> fileInfo.getDstoreSockets().remove(dstore));
    removeFilesInfo.values().forEach(fileInfo -> fileInfo.getDstoreSockets().remove(dstore));
  }
}