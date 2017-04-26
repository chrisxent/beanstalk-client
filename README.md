# beanstalk-client
A Java Beanstalk Client

# Usage

BeansTalkClient producer = new BeansTalkClient(HOST, PORT);
try {
    producer.use("TUBE");
    long jobId = beansTalkClient.put(0, 0, 4, "Test Message".getBytes());
    ...
    producer.close();
    
} catch(BeansTalkException ex) {
}

...

BeansTalkClient consumer = new BeansTalkClient(HOST, PORT);
try {
    consumer.watch("TUBE");
    while (true) {
        BeansTalkJob job = consumer.reserve();
        //...process job
        consumer.delete(job.getJobId());
    }
} catch (BeansTalkException ex) {
}