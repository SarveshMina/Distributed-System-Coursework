import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Index {
  private final ConcurrentHashMap<String, FileState> fileStates = new ConcurrentHashMap<>();

  public void addFile(String filename, List<Socket> dstores, int fileSize) {
    fileStates.put(filename, new FileState(dstores, fileSize, "IN_PROGRESS"));
  }

  public boolean isFileInProgress(String filename) {
    FileState state = fileStates.get(filename);
    return state != null && "IN_PROGRESS".equals(state.status);
  }

  public void markFileInProgress(String filename) {
    FileState state = fileStates.get(filename);
    if (state != null) {
      state.status = "IN_PROGRESS";
    }
  }

  public void markFileAsComplete(String filename) {
    FileState state = fileStates.get(filename);
    if (state != null) {
      state.status = "COMPLETE";
    }
  }

  public void removeFile(String filename) {
    fileStates.remove(filename);
  }

  public FileState getFileState(String filename) {
    return fileStates.get(filename);
  }

  public List<String> getCompleteFiles() {
    return fileStates.entrySet().stream()
        .filter(entry -> "COMPLETE".equals(entry.getValue().getStatus()))
        .map(entry -> entry.getKey())
        .collect(Collectors.toList());
  }

  public static class FileState {
    private List<Socket> dstoreSockets;
    private int fileSize;
    private String status;

    public FileState(List<Socket> dstoreSockets, int fileSize, String status) {
      this.dstoreSockets = dstoreSockets;
      this.fileSize = fileSize;
      this.status = status;
    }

    public List<Socket> getDstoreSockets() {
      return dstoreSockets;
    }

    public void setDstoreSockets(List<Socket> dstoreSockets) {
      this.dstoreSockets = dstoreSockets;
    }

    public int getFileSize() {
      return fileSize;
    }

    public void setFileSize(int fileSize) {
      this.fileSize = fileSize;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }
  }
}