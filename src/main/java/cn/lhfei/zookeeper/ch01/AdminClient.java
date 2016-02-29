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
import java.util.Date;
import java.util.Random;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.lhfei.zookeeper.config.ZKConfiguration;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Feb 26, 2016
 */
public class AdminClient implements Watcher {
	private static final Logger log = LoggerFactory.getLogger(AdminClient.class);
	private static final String CURRENT_NODE = "/zk";

	private Random random = new Random(this.hashCode());
	private ZooKeeper zk;
	private String status;
	private String hostPort;
	private String serverId = Integer.toHexString(random.nextInt());
	private static boolean isLeader = false;

	public AdminClient(String host, int port) {
		this.hostPort = host + ":" + port;
	}

	@Override
	public void process(WatchedEvent event) {
		log.info("{}", event);
	}

	void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 1500, this);
	}

	void listState() throws KeeperException, InterruptedException {
		try {
			Stat stat = new Stat();
			byte masterData[] = zk.getData("/zookeeper", false, stat);
			Date startDate = new Date(stat.getCtime());
			log.info("Master: {}, since {}", new String(masterData), startDate);
		} catch (NoNodeException e) {
			log.info("No Master");
		}
		
		log.info("Workers: ---------------");
		
		for (String w : zk.getChildren("/brokers", false)) {
			byte data[] = zk.getData("/brokers/" + w, false, null);
			
			if(null != data){
				String state = new String(data);
				log.info("\t" + w + ": " + state);
			}
		}
		
		log.info("Tasks:");
		
		for (String t : zk.getChildren("/storm", false)) {
			log.info("\t" + t);
		}
	}
	

	public static void main(String[] args) throws Exception {
		AdminClient c = new AdminClient(ZKConfiguration.ZK_SERVER_80, ZKConfiguration.ZK_CLIENT_PORT);
		c.startZK();
		
		c.listState();
	}

}
