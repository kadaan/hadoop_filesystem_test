package info.kadaan;

import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;
import java.io.InputStream;

public class SeekableFSInputStream extends FSInputStream {
    private InputStream in;
    private long pos;

    public SeekableFSInputStream(InputStream in) {
        this.in = in;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int val = in.read(b, off, len);
        if (val > 0) {
            this.pos += val;
        }
        return val;
    }

    @Override
    public long getPos() throws IOException {
        return this.pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        in.skip(pos);
        this.pos = pos;
    }

    @Override
    public boolean seekToNewSource(long arg0) throws IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        int val = in.read();
        if (val > 0) {
            this.pos += val;
        }
        return val;
    }
}
