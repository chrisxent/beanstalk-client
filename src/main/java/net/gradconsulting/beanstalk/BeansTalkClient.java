package net.gradconsulting.beanstalk;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;

public class BeansTalkClient {

    private final BeansTalkConnection beansTalkConnection;

    public BeansTalkClient(String host, int port) {
        this.beansTalkConnection = new BeansTalkConnection(host, port);
    }

    /* http://kr.github.io/beanstalkd */
    /* https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt */

    /***** Producer Commands *****/

    public long put(long priority, int delay, int ttr, byte[] data) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "put " + priority + " " + delay + " " + ttr + " " + data.length + "\r\n";
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            buf.write(command.getBytes(Charset.defaultCharset()));
            buf.write(data);
            buf.write("\r\n".getBytes(Charset.defaultCharset()));
            controlLine = send(buf.toByteArray());
        } catch (IOException ex) {
            throw new BeansTalkException(ex.getMessage());
        }
        if (!controlLine.startsWith("INSERTED"))
            throw new BeansTalkException("Invalid response in put: " + controlLine);

        return toLong(controlLine.replaceAll("[^0-9]", ""));
    }

    public void use(String tube) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "use " + tube + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (!controlLine.startsWith("USING"))
            throw new BeansTalkException("Invalid response in use: " + controlLine);
    }

    /***** Worker Commands *****/

    public BeansTalkJob reserve() throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "reserve\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        controlLine = send(command.getBytes(Charset.defaultCharset()), CommandHelper.reservedCommandHandler, payload);
        long id = toLong(controlLine.split(" ")[1]);
        return new BeansTalkJob(id, Arrays.copyOf(payload.toByteArray(), payload.toByteArray().length));
    }

    public BeansTalkJob reserve(int timeoutSeconds) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "reserve-with-timeout " + timeoutSeconds + "\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        controlLine = send(command.getBytes(Charset.defaultCharset()), CommandHelper.reservedCommandHandler, payload);
        long id = toLong(controlLine.split(" ")[1]);
        return new BeansTalkJob(id, Arrays.copyOf(payload.toByteArray(), payload.toByteArray().length));
    }

    public void delete(long jobId) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "delete " + jobId + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (controlLine.startsWith("NOT_FOUND"))
            throw new BeansTalkException("Job id not found");

        if (!controlLine.startsWith("DELETED"))
            throw new BeansTalkException("Invalid response in delete: " + controlLine);
    }

    public void release(long id, int priority, int delay) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "release " + id + " " + priority + " " + delay + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (controlLine.startsWith("BURIED"))
            throw new BeansTalkException("job buried. the server ran out of memory");

        if (controlLine.startsWith("NOT_FOUND"))
            throw new BeansTalkException("Job id not found");

        if (!controlLine.startsWith("RELEASED"))
            throw new BeansTalkException("Invalid response in release: " + controlLine);
    }

    public void bury(long jobId, int priority) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "bury " + jobId + " " + priority + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (controlLine.startsWith("NOT_FOUND"))
            throw new BeansTalkException("Job id not found");

        if (!controlLine.startsWith("BURIED"))
            throw new BeansTalkException("Invalid response in bury: " + controlLine);
    }

    public void touch(long jobId) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "touch " + jobId + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (controlLine.startsWith("NOT_FOUND"))
            throw new BeansTalkException("Job id not found");

        if (!controlLine.startsWith("TOUCHED"))
            throw new BeansTalkException("Invalid response in touch: " + controlLine);
    }

    public int watch(String tube) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "watch " + tube + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (!controlLine.startsWith("WATCHING"))
            throw new BeansTalkException("Invalid response in watch: " + controlLine);

        return toInt(controlLine.replaceAll("[^0-9]", ""));
    }

    public int ignore(String tube) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "ignore " + tube + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (controlLine.startsWith("NOT_IGNORED"))
            return 0;

        if (!controlLine.startsWith("WATCHING"))
            throw new BeansTalkException("Invalid response in ignore: " + controlLine);

        return toInt(controlLine.replaceAll("[^0-9]", ""));
    }

    /***** Other Commands *****/

    public String peek(long jobId) throws BeansTalkException {
        this.init();
        String command = "peek " + jobId + "\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        send(command.getBytes(Charset.defaultCharset()), CommandHelper.peekCommandHandler, payload);
        return process(payload);
    }

    public String peekReady() throws BeansTalkException {
        this.init();
        String command = "peek-ready" + "\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        send(command.getBytes(Charset.defaultCharset()), CommandHelper.peekCommandHandler, payload);
        return process(payload);
    }

    public String peekDelayed() throws BeansTalkException {
        this.init();
        String command = "peek-delayed" + "\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        send(command.getBytes(Charset.defaultCharset()), CommandHelper.peekCommandHandler, payload);
        return process(payload);
    }

    public String peekBuried() throws BeansTalkException {
        this.init();
        String command = "peek-buried" + "\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        send(command.getBytes(Charset.defaultCharset()), CommandHelper.peekCommandHandler, payload);
        return process(payload);
    }

    public long kick(int bound) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "kick " + bound + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (!controlLine.startsWith("KICKED"))
            throw new BeansTalkException("Invalid response in watch: " + controlLine);

        return toLong(controlLine.replaceAll("[^0-9]", ""));
    }

    public void kickJob(long jobId) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "kick " + jobId + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (controlLine.startsWith("NOT_FOUND"))
            throw new BeansTalkException("Job not found");

        if (!controlLine.startsWith("KICKED"))
            throw new BeansTalkException("Invalid response in kick: " + controlLine);
    }

    public String statsJob(long jobId) throws BeansTalkException {
        this.init();
        String command = "stats-job " + jobId + "\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        send(command.getBytes(Charset.defaultCharset()), CommandHelper.statsJobCommandHandler, payload);
        return process(payload);
    }

    public String statsTube(String tube) throws BeansTalkException {
        this.init();
        String command = "stats-tube " + tube + "\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        send(command.getBytes(Charset.defaultCharset()), CommandHelper.statsTubeCommandHandler, payload);
        return process(payload);
    }

    public String stats() throws BeansTalkException {
        this.init();
        String statsTubeCommand = "stats\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        send(statsTubeCommand.getBytes(Charset.defaultCharset()), CommandHelper.statsCommandHandler, payload);
        return process(payload);
    }

    public String listTubes() throws BeansTalkException {
        this.init();
        String command = "list-tubes\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        send(command.getBytes(Charset.defaultCharset()), CommandHelper.listTubesCommandHandler, payload);
        return process(payload);
    }

    public String listTubeUsed() throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "list-tube-used" + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (!controlLine.startsWith("USING"))
            throw new BeansTalkException("Invalid response in list-tube-used: " + controlLine);

        String tube = controlLine.split(" ")[1];
        return tube.substring(0, tube.length() - 2);
    }

    public String listTubesWatched() throws BeansTalkException {
        this.init();
        String command = "list-tubes-watched\r\n";
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        send(command.getBytes(Charset.defaultCharset()), CommandHelper.listTubesWatchedCommandHandler, payload);
        return process(payload);
    }

    public void quick() throws BeansTalkException {
        this.close();
    }

    public void pauseTube(String tube, int delay) throws BeansTalkException {
        this.init();
        String controlLine = "";
        String command = "pause-tube " + tube + " " + delay + "\r\n";
        controlLine = send(command.getBytes(Charset.defaultCharset()));
        if (controlLine.startsWith("NOT_FOUND"))
            throw new BeansTalkException("Tube not found");

        if (!controlLine.startsWith("PAUSED"))
            throw new BeansTalkException("Invalid response in pause-tube: " + controlLine);
    }

    /**** End of commands ****/

    public String getHost() {
        return this.beansTalkConnection.getHost();
    }

    public int getPort() {
        return this.beansTalkConnection.getPort();
    }

    public void close() throws BeansTalkException {
        try {
            this.beansTalkConnection.close();
        } catch (IOException e) {
            throw new BeansTalkException(e.getMessage());
        }
    }

    private void init() throws BeansTalkException {
        if (this.beansTalkConnection.isOpen())
            return;
        try {
            this.beansTalkConnection.connect();
        } catch (IOException ex) {
            throw new BeansTalkException(ex.getMessage());
        }
    }

    private String send(byte[] command) throws BeansTalkException {
        try {
            this.beansTalkConnection.write(command);
            return this.beansTalkConnection.readControlLine();
        } catch (IOException ex) {
            throw new BeansTalkException(ex.getMessage());
        }
    }

    private String send(byte[] payload, CommandHandler commandHandler, ByteArrayOutputStream buf) throws BeansTalkException {
        try {
            this.beansTalkConnection.write(payload);
            return this.beansTalkConnection.readControlLine(commandHandler, buf);
        } catch (IOException ex) {
            throw new BeansTalkException(ex.getMessage());
        }
    }

    private String process(ByteArrayOutputStream payload) throws BeansTalkException {
        try {
            return payload.toString(Charset.defaultCharset().toString());
        } catch (UnsupportedEncodingException ex) {
            throw new BeansTalkException(ex.getMessage());
        }
    }

    private long toLong(String value) throws BeansTalkException {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            throw new BeansTalkException(ex.getMessage());
        }
    }

    private int toInt(String value) throws BeansTalkException {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            throw new BeansTalkException(ex.getMessage());
        }
    }
}
