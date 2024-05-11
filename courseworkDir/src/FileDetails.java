import java.net.Socket;
import java.util.List;

public class FileDetails {
  private List<Socket> dstoreSockets;
  private int fileSize;

  public FileDetails(List<Socket> dstoreSockets, int fileSize) {
    this.dstoreSockets = dstoreSockets;
    this.fileSize = fileSize;
  }

  public List<Socket> getDstoreSockets() {
    return dstoreSockets;
  }

  public int getFileSize() {
    return fileSize;
  }
}
