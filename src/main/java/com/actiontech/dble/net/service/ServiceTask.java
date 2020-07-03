package com.actiontech.dble.net.service;

/**
 * Created by szf on 2020/6/18.
 */
public class ServiceTask {

    private final byte[] orgData;
    private volatile boolean highPriority = false;
    private final Service service;

    public ServiceTask(byte[] orgData, Service service) {
        this.orgData = orgData;
        this.service = service;
    }


    public byte[] getOrgData() {
        return orgData;
    }

    public boolean isHighPriority() {
        return highPriority;
    }

    public Service getService() {
        return service;
    }

    public void increasePriority() {
        highPriority = true;
    }


}
