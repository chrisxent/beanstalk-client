package net.gradconsulting.beanstalk;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

class BeansTalkConnection {

    private static final int BUFFER_SIZE = 1024;
    private final String host;
    private final int port;
    private SocketChannel socketChannel;

    public BeansTalkConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        this.socketChannel = SocketChannel.open();
        this.socketChannel.connect(new InetSocketAddress(this.host, this.port));
    }

    public void close() throws IOException {
        this.socketChannel.close();
    }

    public boolean isOpen() {
        return (this.socketChannel != null && this.socketChannel.isOpen());
    }

    public String readControlLine() throws IOException, BeansTalkException {
        /* 1k must be enough for a single response */
        ByteBuffer buf = ByteBuffer.allocate(BUFFER_SIZE);
        int read = this.socketChannel.read(buf);
        if (read > 0) {
            buf.flip();
            for (int i = 0; i < buf.limit() - 1; i++) {
                if (buf.get(i) == '\r' && buf.get(i + 1) == '\n') {
                    String controlLine = new String(Arrays.copyOf(buf.array(), i), Charset.defaultCharset());
                    return controlLine;
                }
            }
        } else {
            throw new BeansTalkException("No data found");
        }
        throw new BeansTalkException("Control line terminator not found");
    }

    public String readControlLine(CommandHandler commandHandler, ByteArrayOutputStream payload) throws IOException,
            BeansTalkException {

        /* 1k must be enough to reach the first line terminator '\r\n' */
        ByteBuffer buf = ByteBuffer.allocate(BUFFER_SIZE);
        String controlLine = null;
        int dataLen, read, dataRem = 0;

        read = this.socketChannel.read(buf);
        if (read < 1)
            throw new BeansTalkException("No data found");

        buf.flip();
        for (int i = 0; i < buf.limit() - 1; i++) {
            if (buf.get(i) == '\r' && buf.get(i + 1) == '\n') {
                controlLine = new String(Arrays.copyOf(buf.array(), i), Charset.defaultCharset());
                dataLen = commandHandler.process(controlLine);
                if (dataLen < 1)
                    return controlLine;

                dataRem = (controlLine.length() + dataLen + 4) - read;
                if (dataRem < 1) {
                    for (int j = i + 2; j < buf.limit() - 2; j++)
                        payload.write(buf.get(j));

                    return controlLine;

                } else if (dataRem == 1)  {
                    for (int j = i + 2; j < buf.limit() - 1; j++)
                        payload.write(buf.get(j));

                } else {
                    for (int j = i + 2; j < buf.limit(); j++)
                        payload.write(buf.get(j));
                }
            }
        }
        if (controlLine == null)
            throw new BeansTalkException("Control line terminator not found");

        buf.clear();
        buf = ByteBuffer.allocate(dataRem);
        int totalRead = 0;

        while ((read = this.socketChannel.read(buf)) != 0) {
            if (read < 1)
                throw new BeansTalkException("No data found");

            totalRead += read;
            buf.flip();
            for (int i = 0; i < buf.limit() - 1; i++)
                if (buf.get(i) != '\r' && buf.get(i + 1) != '\n')
                    payload.write(buf.get(i));

            buf.clear();
            if (totalRead >= dataRem)
                return controlLine;
        }
        throw new BeansTalkException("Connection end");
    }

    public void write(byte[] bytes) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        while (buf.hasRemaining())
            socketChannel.write(buf);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
