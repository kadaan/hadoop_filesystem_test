package info.kadaan;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HadoopUtils {
  public static void renamePath(FileSystem fs, Path oldName, Path newName) throws IOException {
    if (!fs.exists(oldName)) {
      throw new FileNotFoundException(String.format("Failed to rename %s to %s: src not found", oldName, newName));
    }
    if (fs.exists(newName)) {
      throw new FileAlreadyExistsException(
          String.format("Failed to rename %s to %s: dst already exists", oldName, newName));
    }
    if (!fs.rename(oldName, newName)) {
      throw new IOException(String.format("Failed to rename %s to %s", oldName, newName));
    }
  }

  public static void movePath(FileSystem srcFs, Path src, FileSystem dstFs, Path dst) throws IOException {
    if (srcFs.getUri().equals(dstFs.getUri())) {
      renamePath(srcFs, src, dst);
    } else {
      if (!FileUtil.copy(srcFs, src, dstFs, dst, true, false, dstFs.getConf())) {
        throw new IOException(String.format("Failed to move %s to %s", src, dst));
      }
    }
  }
}
