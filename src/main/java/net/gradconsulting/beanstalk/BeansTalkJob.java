package net.gradconsulting.beanstalk;

import java.util.Arrays;

public class BeansTalkJob {

    private final long id;
    private final byte[] data;

    public BeansTalkJob(long id, byte[] data) {
        this.id = id;
        this.data = Arrays.copyOf(data, data.length);
    }

    public long getId() {
        return id;
    }

    public byte[] getData() {
        return Arrays.copyOf(data, data.length);
    }
}
