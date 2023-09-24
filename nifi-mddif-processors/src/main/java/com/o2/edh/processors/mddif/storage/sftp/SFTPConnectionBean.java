package com.o2.edh.processors.mddif.storage.sftp;

import com.jcraft.jsch.*;
import com.o2.edh.processors.mddif.util.Log;
import com.o2.edh.processors.mddif.util.Logger;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SFTPConnectionBean {

    private static Map<String, ActiveSession> cache = new ConcurrentHashMap<>();

    public synchronized static Session newSession(String hostName, String userName, String port, String privateKeyPath, Logger logger) throws JSchException {

        if(cache.containsKey(hostName + port + userName)){
            if(cache.get(hostName + port + userName).getSession().isConnected()) // Channel is connected
                return cache.get(hostName + port + userName).getSession();
            }else {
                cache.remove(hostName + port + userName);
            }

        JSch jsch = new JSch();
        jsch.addIdentity(privateKeyPath);
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        config.put("PreferredAuthentications", "publickey");//added to specify authentication method!
        //set config for alive interval
        Session session;
        session = jsch.getSession(userName, hostName, Integer.parseInt(port));
        session.setConfig(config);

        session.setServerAliveCountMax(4); // send 4 times
        session.setServerAliveInterval(1000*60); // after 1 min
        session.connect(30000); //connection timeout
        session.setTimeout(0); // setting socket (read/write) timeout 0 i.e. unlimited.

        ActiveSession activeSession = new ActiveSession(session,new Date().getTime());
        cache.put(hostName + port + userName, activeSession);

        logger.generateLog(new Log(LogLevel.DEBUG,null,null,null,"110024", // change these ids
                "Created new session for : " + hostName +":"+ port +":"+ userName));

        return session;
    }

    public synchronized static Session getSession(String hostName,String port, String userName, String privateKeyPath, Logger logger) throws JSchException {
        if(cache.containsKey(hostName + port + userName)){
            if(cache.get(hostName + port + userName).getSession().isConnected()){ // Session is connected
                cache.get(hostName + port + userName).setLastUsed(new Date().getTime());
                logger.generateLog(new Log(LogLevel.DEBUG,null,null,null,"110023", // change these ids
                        "Returning active session for : " + hostName +":"+ port +":"+ userName));
                return cache.get(hostName + port + userName).getSession();
            }
        }
        return newSession(hostName, userName, port, privateKeyPath, logger);
    }

    public static void closeInactiveConnection(Logger logger){ //not using this method as we will wait for the source SFTP server to handle closer of inactive connections
        ArrayList<String> keyList = new ArrayList<>();
        Set<Map.Entry<String, ActiveSession>> entrySet = cache.entrySet();
        Iterator<Map.Entry<String, ActiveSession>> itr = entrySet.iterator();

        logger.generateLog(new Log(LogLevel.DEBUG,null,null,null,"110025", // change these ids
                "Inside closeSession: Size of cache : " + entrySet.size()));
        while (itr.hasNext()) {
            Map.Entry<String, ActiveSession> entry = itr.next();
            String key = entry.getKey();
            ActiveSession activeSession = entry.getValue();

            if(activeSession.getLastUsed() < (new Date().getTime()- 14400000 )) { // checking if session is inactive for more than 4 hours.
                keyList.add(key);
                logger.generateLog(new Log(LogLevel.DEBUG,null,null,null,"110026", // change these ids
                        "Inactive Session chosen for close: key: " + entry.getKey() + " session connected: " + entry.getValue().getSession().isConnected() +
                                "   Last used time : "+activeSession.getLastUsed()
                                +"  Current time : " + new Date().getTime()));
            }else {
                logger.generateLog(new Log(LogLevel.DEBUG,null,null,null,"110027", // change these ids
                        "Active session: key: " + entry.getKey() + " session connected: " + entry.getValue().getSession().isConnected() +
                                "   Last used time : "+activeSession.getLastUsed()
                                +"  Current time : " + new Date().getTime()));
            }
        }

        for(int i = 0; i < keyList.size(); i++){
             cache.get(keyList.get(i)).getSession().disconnect();
             cache.remove(keyList.get(i));
        }
    }

    public static void closeAllSessions(ComponentLog logger) throws JSchException {
        Set<Map.Entry<String, ActiveSession>> entrySet = cache.entrySet();
        Iterator<Map.Entry<String, ActiveSession>> itr = entrySet.iterator();

        logger.log(LogLevel.DEBUG,"Inside closeAllSessions Size of cache : " + entrySet.size());
        while (itr.hasNext()) {
            Map.Entry<String, ActiveSession> entry = itr.next();
            logger.log(LogLevel.DEBUG,"closeAllSession: key: " + entry.getKey() + " session connected: "
                    + entry.getValue().getSession().isConnected());
            entry.getValue().getSession().disconnect();
        }
        cache.clear();
    }
}