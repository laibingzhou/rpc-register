package cn.bingzhou.rpcregister;

import java.util.concurrent.CountDownLatch;







import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceRegister {
	
	private Logger logger=LoggerFactory
			.getLogger(ServiceRegister.class);
	private ZooKeeper zoo=null;
	
	CountDownLatch count=new CountDownLatch(1);
	
	private String address;
	
	public ServiceRegister(String address) throws Exception{
		this.address=address;
		connect();
	}
	
	private void connect() throws Exception{
		zoo=new ZooKeeper(address, 2000, new Watcher(){

			public void process(WatchedEvent event) {
				if(event.getState()==KeeperState.SyncConnected){
					count.countDown();
				}
			}
		});
		count.await();
	}

	/**
	 * 将服务类注册于各个节点下
	 * @param ip 
	 * @throws Exception
	 */
	public void register(String ip) throws Exception {
		if(zoo.exists(Contants.REGISTER_ADDRESS, null)==null){
			String create = zoo.create(Contants.REGISTER_ADDRESS, null, Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			logger.debug("create path:{}",create);
		}
		String dataPath = zoo.create("/register", ip.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		logger.debug("create data path:",dataPath);
	}
	
	public void close() throws Exception{
		zoo.close();
	}

	
	

}
