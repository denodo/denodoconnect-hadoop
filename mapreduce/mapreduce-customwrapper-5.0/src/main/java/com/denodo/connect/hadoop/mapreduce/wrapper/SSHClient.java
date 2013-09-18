package com.denodo.connect.hadoop.mapreduce.wrapper;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;


public final class SSHClient {

    private static final Logger logger = Logger.getLogger(SSHClient.class);

    private Session session;
    private int timeout;
    private String executionOutput;
    private String executionError;
    private int exitStatus;

    public SSHClient(String host, int port, String user, int timeout) throws JSchException {

        JSch jsch = new JSch();
        init(host, port, user, timeout, jsch);

    }

    public SSHClient(String host, int port, String user, int timeout, String keyFile,
        String passphrase) throws JSchException {

        JSch jsch = new JSch();
        jsch.addIdentity(keyFile, passphrase);
        init(host, port, user, timeout, jsch);
    }

    private void init(String host, int port, String user, int t, JSch jsch)
        throws JSchException {

        this.session = jsch.getSession(user, host, port);
        this.session.setConfig("StrictHostKeyChecking", "no");
        this.timeout = t;
    }

    public void setPassword(String password) {
        this.session.setPassword(password);
    }

    public int execute(String command) throws JSchException, IOException {


        try {

            this.session.connect(this.timeout);
            logger.debug("Openning channel...");
            ChannelExec channel = (ChannelExec) this.session.openChannel("exec");
            channel.setCommand(command);

            channel.setInputStream(null);
            channel.setErrStream(System.err);

            InputStream in = channel.getInputStream();
            InputStream err = channel.getErrStream();
            channel.connect(this.timeout);

            this.executionOutput = IOUtils.toString(in);
            this.executionError = IOUtils.toString(err);

            this.exitStatus = channel.getExitStatus();
            logger.debug("Exit status: " + this.exitStatus);

            return this.exitStatus;

        } finally {
            if (this.session != null) {
                this.session.disconnect();
            }
        }

    }

    public String getExecutionError() {
        return this.executionError;
    }

    public String getExecutionOutput() {
        return this.executionOutput;
    }

}
