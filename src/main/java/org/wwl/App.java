package org.wwl;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.event.SwingPropertyChangeSupport;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.InvalidConfigException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws UnknownHostException, InvalidConfigException
    {
        // TODO release pool at exit
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMinConnSize(10);
        nebulaPoolConfig.setMaxConnSize(1000);
        nebulaPoolConfig.setIdleTime(60000);
        nebulaPoolConfig.setWaitTime(60000);
        nebulaPoolConfig.setTimeout(60000);
        nebulaPoolConfig.setIntervalIdle(300000);
        List<HostAddress> addresses = new ArrayList<>();
        String []hosts = {"192.168.15.33"};
        for (String h : hosts) {
            addresses.add(new HostAddress(h, 8663));
        }

        final NebulaPool pool = new NebulaPool();
        pool.init(addresses, nebulaPoolConfig);

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Thread t = new Thread(new Runnable() {
                private void doRun() throws NotValidConnectionException, IOErrorException, AuthFailedException, ClientServerIncompatibleException {
                    Session session = null;

                    try {
                        session = pool.getSession("root", "nebula", false);
                        ResultSet rs = session.execute("use test; match (v) return v limit 1;");
                        if (rs == null) {
                            System.err.println( System.currentTimeMillis() + ": result set is nil");
                        } else if (!rs.isSucceeded()) {
                            System.err.println(System.currentTimeMillis() +  ": graph failed: " + rs.getErrorMessage());
                        } else {
                            System.out.println( System.currentTimeMillis() + ": succ done");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (session != null) {
                            session.release();
                        }
                    }

                }

                @Override
                public void run() {
                    try {
                        while (true) {
                            doRun();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            t.start();
            workers.add(t);
        }

        for (Thread t : workers) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println( "Hello World!" );
    }
}
