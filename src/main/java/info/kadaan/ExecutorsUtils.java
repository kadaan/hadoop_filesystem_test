package info.kadaan;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ExecutorsUtils {

    public static final long EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT = 60;
    public static final TimeUnit EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;

    public static ThreadFactory newThreadFactory(Optional<String> nameFormat) {
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        if (nameFormat.isPresent()) {
            builder.setNameFormat(nameFormat.get());
        }
        return builder.setUncaughtExceptionHandler(new LoggingUncaughtExceptionHandler()).build();
    }

    public static void shutdownExecutorService(ExecutorService executorService, long timeout, TimeUnit unit) {
        Preconditions.checkNotNull(unit);
        // Disable new tasks from being submitted
        executorService.shutdown();

        try {
            long halfTimeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit) / 2;
            // Wait for half the duration of the timeout for existing tasks to terminate
            if (!executorService.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)) {
                // Cancel currently executing tasks
                executorService.shutdownNow();

                // Wait the other half of the timeout for tasks to respond to being cancelled
                executorService.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException ie) {
            // Preserve interrupt status
            Thread.currentThread().interrupt();
            // (Re-)Cancel if current thread also interrupted
            executorService.shutdownNow();
        }
    }

    public static void shutdownExecutorService(ExecutorService executorService) {
        shutdownExecutorService(executorService, EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT,
                EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT_TIMEUNIT);
    }
}