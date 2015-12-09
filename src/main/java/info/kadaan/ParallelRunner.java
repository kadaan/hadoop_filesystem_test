package info.kadaan;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

public class ParallelRunner implements Closeable {
    private final ExecutorService executor;
    private final FileSystem fs;

    private final List<Future<?>> futures = Lists.newArrayList();

    private final Striped<Lock> locks = Striped.lazyWeakLock(Integer.MAX_VALUE);

    public ParallelRunner(int threads, FileSystem fs) {
        this.executor = Executors.newFixedThreadPool(threads,
                ExecutorsUtils.newThreadFactory(Optional.of("ParallelRunner")));
        this.fs = fs;
    }

    public void movePath(final Path src, final FileSystem dstFs, final Path dst) {
        this.futures.add(this.executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Lock lock = locks.get(src.toString());
                lock.lock();
                try {
                    if (fs.exists(src)) {
                        HadoopUtils.movePath(fs, src, dstFs, dst);
                    }
                    return null;
                } catch (FileAlreadyExistsException e) {
                    return null;
                } finally {
                    lock.unlock();
                }
            }
        }));
    }

    @Override
    public void close() throws IOException {
        try {
            // Wait for all submitted tasks to complete
            for (Future<?> future : this.futures) {
                future.get();
            }
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        } catch (ExecutionException ee) {
            throw new IOException(ee);
        } finally {
            ExecutorsUtils.shutdownExecutorService(this.executor);
        }
    }
}
