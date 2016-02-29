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

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
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
public class Worker implements Watcher {
	private static final Logger log = LoggerFactory.getLogger(Worker.class);
	private static final String CURRENT_NODE = "/zk";

	private Random random = new Random(this.hashCode());
	private ZooKeeper zk;
	private String status;
	private String hostPort;
	private String serverId = Integer.toHexString(random.nextInt());
	private static boolean isLeader = false;

	public Worker(String host, int port) {
		this.hostPort = host + ":" + port;
	}

	@Override
	public void process(WatchedEvent event) {
		log.info("{}", event);
	}

	void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 1500, this);
	}

	void register() throws KeeperException, InterruptedException {
		zk.create(CURRENT_NODE, "HEFEI-LI".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
				stringCallback, null);
	}

	StringCallback stringCallback = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				try {
					register();

					log.info("Registered successfully: {}", serverId);
				} catch (KeeperException | InterruptedException e) {
					log.error(e.getMessage(), e);
				}
				break;
			case OK:
				log.warn("Already registered: {}", serverId);
				break;
			case NODEEXISTS:

				break;
			default:
				log.error("Something went wrong: {}", KeeperException.create(Code.get(rc), path));
			}
		}
	};
	
	StatCallback statusUpdateCallback = new StatCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				updateStatus((String) ctx);
				return;
			}
		}
	};
	
	synchronized private void updateStatus(String status) {
		if (status == this.status) {
			zk.setData(CURRENT_NODE, status.getBytes(), -1, statusUpdateCallback, status);
		}
	}

	public void setStatus(String status) {
		this.status = status;
		updateStatus(status);
	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		Worker w = new Worker(ZKConfiguration.ZK_SERVER_80, ZKConfiguration.ZK_CLIENT_PORT);
		w.startZK();
		w.register();
		Thread.sleep(30000);
	}

}
