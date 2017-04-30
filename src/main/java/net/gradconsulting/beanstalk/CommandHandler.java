package net.gradconsulting.beanstalk;

interface CommandHandler {
    int process(String controlLine) throws BeansTalkException;
}
