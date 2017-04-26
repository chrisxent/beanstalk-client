package net.gradconsulting.beanstalk;

interface CommandHandler {
    int parse(String controlLine) throws BeansTalkException;
}
