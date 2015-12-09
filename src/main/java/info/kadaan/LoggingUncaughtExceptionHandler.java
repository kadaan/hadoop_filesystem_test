package info.kadaan;

public class LoggingUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        System.out.println(String.format("Thread %s threw an uncaught exception: %s", t, e));
    }
}

