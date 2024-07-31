import java.net.Socket;
import java.util.List;

/**
 * The FileInfo class represents the state of a file in the system.
 * It contains information about the Dstores that store the file, the file size, and the status of the file.
 */
public class FileInfo {
  private List<Socket> dstoreSockets; // List of Dstores storing the file
  public int fileSize; // Size of the file
  public String statusInfo; // Status of the file (e.g., IN_PROGRESS, COMPLETE, REMOVE_IN_PROGRESS)

  /**
   * Constructor to initialize FileInfo.
   *
   * @param dstoreSockets the list of Dstores storing the file
   * @param fileSize      the size of the file
   * @param status        the status of the file
   */
  public FileInfo(List<Socket> dstoreSockets, int fileSize, String status) {
    this.dstoreSockets = dstoreSockets;
    this.fileSize = fileSize;
    this.statusInfo = status;
  }

  /**
   * Gets the list of Dstores storing the file.
   *
   * @return the list of Dstore sockets
   */
  public List<Socket> getDstoreSockets() {
    return dstoreSockets;
  }

  /**
   * Sets the status of the file.
   *
   * @param status the new status of the file
   */
  public void setStatus(String status) {
    this.statusInfo = status;
  }

  /**
   * Gets the size of the file.
   *
   * @return the size of the file
   */
  public int getFileSize() {
    return fileSize;
  }

  /**
   * Gets the status of the file.
   *
   * @return the status of the file
   */
  public String getStatusInfo() {
    return statusInfo;
  }
}