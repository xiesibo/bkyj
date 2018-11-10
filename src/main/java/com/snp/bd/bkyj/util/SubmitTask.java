package com.snp.bd.bkyj.util;

import com.snp.bd.bkyj.job.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 1更新任务状态
 * 2提交任务
 * 3更新任务执行结果
 * Created by x on 2018/8/3.
 */
public class SubmitTask implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(SubmitTask.class);
    private String cmd;
    private int id;
    private RemoteConnection rc;

    public SubmitTask(String cmd, int id) {
        this.cmd = cmd;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public SubmitTask() {
    }

    public String getCmd() {
        return cmd;
    }

    public void setCmd(String cmd) {
        this.cmd = cmd;
    }

    @Override
    public void run() {
        rc = new RemoteConnection(Config.SUBMIT_HOST, Config.SUBMIT_HOST_USER, Config.SUBMIT_PASSWORD);
        int state = JobService.JOB_STATE_ERROR;
        try {
            JobService.updateJobState(id, JobService.JOB_STATE_RUNNING);
            if (rc.execCommand(cmd)) {
                state = JobService.JOB_STATE_SUCCESS;
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            try {
                JobService.updateJobState(id, state);
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

}
