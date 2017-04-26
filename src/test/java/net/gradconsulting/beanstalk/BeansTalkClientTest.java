package net.gradconsulting.beanstalk;

import org.junit.Assert;
import org.junit.Test;
import java.nio.charset.Charset;

public class BeansTalkClientTest {

    private static final int PORT = 11300;
    private static final String HOST = "localhost";
    private static final String TEST_TUBE = "TEST-TUBE";
    private static final String TEST_MSG = "Test Message";

    /* beanstalk demon must be restarted before run this test and no
     * other client must be connected.
     */

    /* test01
     * Test: put, use, reserve, watch and delete with two client instance.
     */
    @Test
    public void test01() {
        BeansTalkClient producer = new BeansTalkClient(HOST, PORT);
        BeansTalkClient consumer = new BeansTalkClient(HOST, PORT);
        try {
            producer.use(TEST_TUBE);
            long jobId = producer.put(0, 0, 4, TEST_MSG.getBytes(Charset.defaultCharset()));
            Assert.assertTrue(jobId > 0);
            producer.close();

            int wn = consumer.watch(TEST_TUBE);
            Assert.assertTrue(wn > 0);
            BeansTalkJob job = consumer.reserve();
            Assert.assertTrue(job.getId() == jobId);
            consumer.delete(job.getId());
            consumer.close();
        } catch (BeansTalkException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }

    /* test02
     * Test: put, use, reserve, watch and delete with one client instance.
     */
    @Test
    public void test02() {
        BeansTalkClient beansTalkClient = new BeansTalkClient(HOST, PORT);
        try {
            beansTalkClient.use(TEST_TUBE);
            long jobId = beansTalkClient.put(0, 0, 4, TEST_MSG.getBytes(Charset.defaultCharset()));
            Assert.assertTrue(jobId > 0);

            int wn = beansTalkClient.watch(TEST_TUBE);
            Assert.assertTrue(wn > 0);
            BeansTalkJob job = beansTalkClient.reserve();
            Assert.assertTrue(job.getId() == jobId);
            beansTalkClient.delete(job.getId());
            beansTalkClient.close();
        } catch (BeansTalkException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }

    @Test(timeout = 6500)
    public void reserveWithTimeOutTest() {
        BeansTalkClient beansTalkClient = new BeansTalkClient(HOST, PORT);
        try {
            beansTalkClient.watch(TEST_TUBE);
            beansTalkClient.reserve(5);
            beansTalkClient.close();
            Assert.fail("Expected an BeansTalkException (time out) to be thrown");
        } catch (BeansTalkException e) {
            Assert.assertTrue(e.getMessage().contains("Time out"));
        }
    }

    @Test
    public void deadLineSoonTest() {
        BeansTalkClient producer = new BeansTalkClient(HOST, PORT);
        BeansTalkClient consumer = new BeansTalkClient(HOST, PORT);
        long jobId = 0;
        try {
            producer.use(TEST_TUBE);
            jobId = producer.put(0, 0, 4, TEST_MSG.getBytes(Charset.defaultCharset()));
            producer.close();

            consumer.watch(TEST_TUBE);
            consumer.reserve();
            Thread.sleep(3000);
            consumer.reserve();
            Assert.fail("Expected an BeansTalkDeadLineSoonException to be thrown");

        } catch (BeansTalkException e) {
            Assert.assertTrue(e.getMessage().contains("Dead line soon"));
            try {
                consumer.delete(jobId);
            } catch (BeansTalkException e1) {
                Assert.fail("Job could not be deleted");
            }
        } catch (InterruptedException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }

    @Test
    public void releaseTest() {
        BeansTalkClient producer = new BeansTalkClient(HOST, PORT);
        BeansTalkClient consumer = new BeansTalkClient(HOST, PORT);
        long jobId;
        try {
            producer.use(TEST_TUBE);
            jobId = producer.put(0, 0, 4, TEST_MSG.getBytes(Charset.defaultCharset()));
            producer.close();

            consumer.watch(TEST_TUBE);
            consumer.reserve();
            consumer.release(jobId, 0, 0);
            BeansTalkJob job = consumer.reserve();
            Assert.assertTrue(job.getId() == jobId);
            consumer.delete(job.getId());
            consumer.close();
        } catch (BeansTalkException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }

    @Test
    public void buryAndKickBoundTest() {
        BeansTalkClient beansTalkClient = new BeansTalkClient(HOST, PORT);
        long jobId;
        try {
            beansTalkClient.use(TEST_TUBE);
            jobId = beansTalkClient.put(0, 0, 4, TEST_MSG.getBytes(Charset.defaultCharset()));
            beansTalkClient.watch(TEST_TUBE);
            beansTalkClient.reserve();
            beansTalkClient.bury(jobId, 0);
            long r = beansTalkClient.kick(1);
            Assert.assertTrue(r == 1);
            beansTalkClient.delete(jobId);
            beansTalkClient.close();
        } catch (BeansTalkException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }

    @Test
    public void buryAndKickTest() {
        BeansTalkClient beansTalkClient = new BeansTalkClient(HOST, PORT);
        long jobId;
        try {
            beansTalkClient.use(TEST_TUBE);
            jobId = beansTalkClient.put(0, 0, 4, TEST_MSG.getBytes(Charset.defaultCharset()));
            beansTalkClient.watch(TEST_TUBE);
            beansTalkClient.reserve();
            beansTalkClient.bury(jobId, 0);
            beansTalkClient.kickJob(jobId);
            beansTalkClient.delete(jobId);
            beansTalkClient.close();
        } catch (BeansTalkException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }

    @Test
    public void touchTest() {
        BeansTalkClient beansTalkClient = new BeansTalkClient(HOST, PORT);
        long jobId;
        try {
            beansTalkClient.use(TEST_TUBE);
            jobId = beansTalkClient.put(0, 0, 2, TEST_MSG.getBytes(Charset.defaultCharset()));
            beansTalkClient.watch(TEST_TUBE);
            beansTalkClient.reserve();
            beansTalkClient.touch(jobId);
            beansTalkClient.delete(jobId);
            beansTalkClient.close();
        } catch (BeansTalkException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }

    @Test
    public void peekTest() {
        BeansTalkClient beansTalkClient = new BeansTalkClient(HOST, PORT);
        long jobId;
        try {
            beansTalkClient.use(TEST_TUBE);
            jobId = beansTalkClient.put(0, 0, 4, TEST_MSG.getBytes(Charset.defaultCharset()));
            Assert.assertTrue(beansTalkClient.peek(jobId).equals(TEST_MSG));
            beansTalkClient.watch(TEST_TUBE);
            beansTalkClient.delete(jobId);
            beansTalkClient.close();
        } catch (BeansTalkException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }

    @Test
    public void peekReadyTest() {
        BeansTalkClient beansTalkClient = new BeansTalkClient(HOST, PORT);
        long jobId;
        try {
            beansTalkClient.use(TEST_TUBE);
            jobId = beansTalkClient.put(0, 0, 4, TEST_MSG.getBytes(Charset.defaultCharset()));
            Assert.assertTrue(beansTalkClient.peekReady().equals(TEST_MSG));
            beansTalkClient.watch(TEST_TUBE);
            beansTalkClient.delete(jobId);
            beansTalkClient.close();
        } catch (BeansTalkException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }

    @Test
    public void peekBuriedTest() {
        BeansTalkClient beansTalkClient = new BeansTalkClient(HOST, PORT);
        long jobId;
        try {
            beansTalkClient.use(TEST_TUBE);
            jobId = beansTalkClient.put(0, 0, 4, TEST_MSG.getBytes(Charset.defaultCharset()));

            beansTalkClient.watch(TEST_TUBE);
            beansTalkClient.reserve();
            beansTalkClient.bury(jobId, 0);

            beansTalkClient.use(TEST_TUBE);
            Assert.assertTrue(beansTalkClient.peekBuried().equals(TEST_MSG));

            beansTalkClient.watch(TEST_TUBE);
            beansTalkClient.kickJob(jobId);
            beansTalkClient.delete(jobId);
            beansTalkClient.close();

        } catch (BeansTalkException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }

    @Test
    public void peekDelayedTest() {
        BeansTalkClient beansTalkClient = new BeansTalkClient(HOST, PORT);
        try {
            beansTalkClient.use(TEST_TUBE);
            Assert.assertTrue(beansTalkClient.peekDelayed().equals(""));
            beansTalkClient.close();
        } catch (BeansTalkException e) {
            Assert.fail("Unexpected BeansTalkException: " + e.getMessage());
        }
    }
}
