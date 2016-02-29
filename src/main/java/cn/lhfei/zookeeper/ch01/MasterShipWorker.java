/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.lhfei.zookeeper.ch01;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Feb 26, 2016
 */
public class MasterShipWorker implements Watcher {

	private static final Logger log = LoggerFactory.getLogger(MasterShipWorker.class);
	
	private Random random = new Random(this.hashCode());
    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString( random.nextInt() );
	private static boolean isLeader = false;
	
	
	public MasterShipWorker(String hostPort) {
		this.hostPort = hostPort;
	}
	
	void startZK() {
		try {
			zk = new ZooKeeper(hostPort, 15000, this);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}
	
    /**
     * Closes the ZooKeeper session.
     *
     * @throws IOException
     */
    void stopZK() throws InterruptedException, IOException {
        zk.close();
    }
	
	// returns true if there is a master
	boolean checkMaster() throws KeeperException, InterruptedException {
		while (true) {
			try {
				Stat stat = new Stat();
				byte data[] = zk.getData("/master", false, stat);
				isLeader = new String(data).equals(serverId);
				return true;
			} catch (NoNodeException e) {
				// no master, so try create again
				log.error(e.getMessage(), e);
				
				return false;
			} catch (ConnectionLossException e) {
				log.error(e.getMessage(), e);
			}
		}
	}

	void runForMaster() throws InterruptedException, KeeperException {
		while (true) {
			try {
				zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				isLeader = true;
				break;
			} catch (NodeExistsException e) {
				isLeader = false;
				break;
			} catch (ConnectionLossException e) {
			}
			if (checkMaster())
				break;
		}
	}

	@Override
	public void process(WatchedEvent e) {
		log.info(e.toString());
	}

	public static void main(String[] args) throws InterruptedException, IOException, KeeperException{
		MasterShipWorker m = new MasterShipWorker("basic.internal.hadoop.10-148-10-81.scloud.letv.com:2181");
		m.startZK();
		
		m.runForMaster();
		
		if (isLeader) {
			log.info("I'm the leader");
			// wait for a bit
			Thread.sleep(60000);
		} else {
			log.info("Someone else is the leader");
		}
		m.stopZK();
	}

}
