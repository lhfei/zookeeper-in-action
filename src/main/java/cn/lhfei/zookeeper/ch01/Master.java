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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Feb 26, 2016
 */
public class Master implements Watcher {
	private static final Logger log = LoggerFactory.getLogger(Master.class);
	
	private ZooKeeper zk;
	private String hostPort;
	
	public Master(String hostPort) {
		this.hostPort = hostPort;
	}
	
	void startZK() {
		try {
			zk = new ZooKeeper(hostPort, 15000, this);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}

	@Override
	public void process(WatchedEvent e) {
		log.info(e.toString());
	}

	public static void main(String[] args) throws InterruptedException{
		Master m = new Master("basic.internal.hadoop.10-148-10-81.scloud.letv.com:2181");
		m.startZK();
		
		// wait for a bit
		Thread.sleep(60000);
	}
}
