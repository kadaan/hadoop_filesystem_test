package info.kadaan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopUtilsTest {
    @Test
    public void testMovePath() throws IOException, URISyntaxException {
        String expected = "test";
        ByteArrayOutputStream actual = new ByteArrayOutputStream();

        Path src = new Path("/src/file.test");
        Path dst = new Path("/dst/file.test");
        FileSystem fs1 = Mockito.mock(FileSystem.class);
        //Mockito.when(fs1.exists(src)).thenReturn(true);
        Mockito.when(fs1.getUri()).thenReturn(new URI("fs1:////"));
        Mockito.when(fs1.getFileStatus(src)).thenReturn(new FileStatus(1, false, 1, 1, 1, src));
        Mockito.when(fs1.open(src))
                .thenReturn(new FSDataInputStream(new SeekableFSInputStream(new ByteArrayInputStream(expected.getBytes()))));
        Mockito.when(fs1.delete(src, true)).thenReturn(true);

        FileSystem fs2 = Mockito.mock(FileSystem.class);
        Mockito.when(fs2.exists(dst)).thenReturn(false);
        Mockito.when(fs2.getUri()).thenReturn(new URI("fs2:////"));
        Mockito.when(fs2.getConf()).thenReturn(new Configuration());
        Mockito.when(fs2.create(dst, false)).thenReturn(new FSDataOutputStream(actual, null));

        HadoopUtils.movePath(fs1, src, fs2, dst);

        //Mockito.verify(fs1, Mockito.times(1)).exists(src);
        Mockito.verify(fs1, Mockito.times(1)).getUri();
        Mockito.verify(fs1, Mockito.times(1)).getFileStatus(src);
        Mockito.verify(fs1, Mockito.times(1)).open(src);
        Mockito.verify(fs1, Mockito.times(1)).delete(src, true);
        Mockito.verify(fs2, Mockito.times(1)).exists(dst);
        Mockito.verify(fs2, Mockito.times(1)).getUri();
        Mockito.verify(fs2, Mockito.times(1)).getConf();
        Mockito.verify(fs2, Mockito.times(1)).create(dst, false);
        Mockito.verifyNoMoreInteractions(fs1, fs2);
        Assert.assertEquals(actual.toString(), expected);
    }
}