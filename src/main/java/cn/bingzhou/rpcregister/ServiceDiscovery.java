package cn.bingzhou.rpcregister;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceDiscovery {
	
	Logger logger=LoggerFactory.getLogger(ServiceDiscovery.class);
	
	private String address;
	
	private List<String> datalist=new ArrayList<String>();
	
	private CountDownLatch count=new CountDownLatch(1);
	public ServiceDiscovery(String address){
		this.address=address;
		ZooKeeper zoo=connectedServer();
		getData(zoo);
	}

	
	private ZooKeeper connectedServer(){
		ZooKeeper zoo=null;
		try{
			zoo=new ZooKeeper(address, 1200, new Watcher(){
	
				public void process(WatchedEvent event) {
					if(KeeperState.SyncConnected==event.getState()){
						count.countDown();
					}
				}}
			);
			count.await();
		}catch(Exception e){
			logger.error("ServiceDiscovery connected error:{}",e);
		}
		return zoo;
	}
	
	public void getData(final ZooKeeper zook) {
		try {
			List<String> children = zook.getChildren("/register", new Watcher(){

				public void process(WatchedEvent event) {
					if(event.getType()==Event.EventType.NodeChildrenChanged){
						try {
							getData(zook);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				
			});
			List<String> datalist=new ArrayList<String>();
			for(String s :children){
				byte[] data = zook.getData("/register"+s,false,null );
				String msg = new String(data);
				datalist.add(msg);
			}
			this.datalist=datalist;
		} catch (Exception e) {
			logger.error("error msg:{}",e);
		}
		
	}


	/**
	 * 提取出zookeeper中的服务器地址
	 * @return
	 */
	public String discover() {
		
		if(datalist.size()<0){
			return null;
		}
		if(datalist.size()==1){
			return datalist.get(0);
		}
		int size = datalist.size();
		Random random = new Random();
		int nextInt = random.nextInt(size);
		return datalist.get(nextInt);
	}
	
	
}
