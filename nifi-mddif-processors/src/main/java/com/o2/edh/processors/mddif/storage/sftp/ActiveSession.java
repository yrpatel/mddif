package com.o2.edh.processors.mddif.storage.sftp;

import com.jcraft.jsch.Session;

public class ActiveSession {
    private Session session;
    private long LastUsed;

    ActiveSession(Session session, long lastUsed) {
        this.session = session;
        LastUsed = lastUsed;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    long getLastUsed() {
        return LastUsed;
    }

    void setLastUsed(long lastUsed) {
        LastUsed = lastUsed;
    }

}
