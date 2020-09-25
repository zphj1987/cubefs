package io.chubao.fs.sdk;

import org.junit.Assert;
import org.junit.Test;

public class AppendFileTest extends StorageTest {

    @Test
    public void testAppendFile() {
        Assert.assertTrue(mkdirs(appendTestDir));
        String path1 = appendTestDir + "/f0";
        Assert.assertTrue(createFile(path1, 0));
        Assert.assertTrue(appendFile(path1));
        CFSStatInfo stat = stat(path1);
        Assert.assertNotNull(stat);
        checkFileStat(stat);
        Assert.assertTrue(appendFile(path1));
        Assert.assertTrue(rmdir(appendTestDir, true));
    }

    @Test
    public void testCreateFileParentNotExist() {
        String path1 = appendTestDir + "/d0/f0";
        Assert.assertFalse(appendFile(path1));
    }

    @Test
    public void testListInvalidPath() {
        String path1 = "../";
        Assert.assertFalse(appendFile(path1));

        String path2 = "/../";
        Assert.assertFalse(appendFile(path2));

        String path3 = null;
        Assert.assertFalse(appendFile(path3));

        String path4 = " ";
        Assert.assertFalse(appendFile(path4));
    }
}