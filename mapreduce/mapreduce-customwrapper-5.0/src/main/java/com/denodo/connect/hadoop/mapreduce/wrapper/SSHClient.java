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
    private String executionOutput;
    private String executionError;

    public SSHClient(String host, int port, String user) throws JSchException {

        JSch jsch = new JSch();
        init(host, port, user, jsch);

    }

    public SSHClient(String host, int port, String user, String keyFile,
        String passphrase) throws JSchException {

        JSch jsch = new JSch();
        jsch.addIdentity(keyFile, passphrase);
        init(host, port, user, jsch);
    }

    private void init(String host, int port, String user, JSch jsch)
        throws JSchException {

        this.session = jsch.getSession(user, host, port);
        this.session.setConfig("StrictHostKeyChecking", "no");
    }

    public void setPassword(String password) {
        this.session.setPassword(password);
    }

    public int execute(String command) throws JSchException, IOException {


        try {

            this.session.connect();
            logger.debug("Openning channel...");
            ChannelExec channel = (ChannelExec) this.session.openChannel("exec");
            channel.setCommand(command);

            channel.setInputStream(null);
            channel.setErrStream(System.err);

            InputStream in = channel.getInputStream();
            InputStream err = channel.getErrStream();
            channel.connect();

            this.executionOutput = IOUtils.toString(in);
            this.executionError = IOUtils.toString(err);

            int exitStatus = channel.getExitStatus();
            logger.debug("Exit status: " + exitStatus);

            return exitStatus;

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
