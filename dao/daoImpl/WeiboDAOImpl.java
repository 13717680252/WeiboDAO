package cn.edu.bjtu.weibo.dao;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cn.edu.bjtu.weibo.model.Weibo;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.*;
import java.util.HashSet;
import junit.framework.TestCase;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.junit.Test;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


import redis.clients.jedis.exceptions.JedisConnectionException;




import org.springframework.stereotype.Repository;
@Repository("weiboDAO")
public class WeiboDAOImpl implements WeiboDAO {
	Jedis jedis;

	public WeiboDAOImpl() {
		final Set<String> sentinels = new HashSet<String>();
		final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setTestOnReturn(true);
		config.setTestOnBorrow(true);

		sentinels.add("121.42.193.80:26379");
		sentinels.add("121.42.193.80:26378");
		SentinelBasedJedisPoolWrapper pool = new SentinelBasedJedisPoolWrapper(config, 90000, null, 0, "mymaster", sentinels);

		jedis = pool.getResource();
		pool.returnResource(jedis);
		pool.destroy();
	}

	@Override
	public String getOwner(String weiboId) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return null;
		String key = "weibo:"+weiboId + ":owner";
		return jedis.get(key);
	}

	@Override
	public String getContent(String weiboId) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return null;
		String key ="weibo:"+ weiboId + ":content";
		return jedis.get(key);
	}

	@Override
	public boolean updateContent(String weiboId, String content) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return false;
		String s = jedis.set("weibo:"+weiboId + ":content", content);
		System.out.println(s);
		if (s .equals("OK"))
			return true;
		else return false;
	}

	@Override
	public String getSendTime(String weiboId) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return null;
		
		String key = "weibo:"+weiboId + ":sendTime";
		return jedis.get(key);
	}


	@Override
	public int getLikeNumber(String weiboId) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return -1;
		String key = "weibo:"+weiboId + ":likeNumber";
		return Integer.valueOf(jedis.get(key)).intValue();
	}

	@Override
	public int getCommentNumber(String weiboId) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return -1;
		String key = "weibo:"+weiboId + ":commentNumber";
		return Integer.valueOf(jedis.get(key)).intValue();
	}

	@Override
	public int getForwardNumber(String weiboId) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return -1;
		String key = "weibo:"+weiboId + ":forwardNumber";
		return Integer.valueOf(jedis.get(key)).intValue();
	}


	@Override
	public boolean deleteWeibo(String weiboId) {
		Set<String> set = jedis.keys("weibo:"+weiboId + ":*");
		Iterator<String> it = set.iterator();
		int count = 0;
		while (it.hasNext()) {
			String keyStr = it.next();
			System.out.println(keyStr);
			jedis.del(keyStr);
			count++;
		}
        
		if (count == 0)
			return false;
		jedis.lrem("weiboList", 1, weiboId);
		return true;
	}

	@Override
	public List<String> getForwardList(String weiboId, int pageIndex,
			int numberPerPage) {
		String key = "weibo:"+weiboId + ":forward";
		List<String> list = jedis.lrange(key, 0, -1);
		int start=pageIndex*numberPerPage;
	    int end=start+numberPerPage;
	    if(end>=list.size())
	    	end=list.size();
	    if(pageIndex<0||numberPerPage<=0||end>=list.size()){
	    	return null;
	    }
	    
		return list.subList(start, end);
	}

	@Override
	public List<String> getCommentList(String weiboId, int pageIndex,
			int numberPerPage) {
		String key = "weibo:"+weiboId + ":comment";
		List<String> list = jedis.lrange(key, 0, -1);
		int start=pageIndex*numberPerPage;
	    int end=start+numberPerPage;
	    if(end>=list.size())
	    	end=list.size();
	    if(pageIndex<0||numberPerPage<=0||end>=list.size()){
	    	return null;
	    }
	    
		return list.subList(start, end);
	}

	@Override
	public boolean insertWeiboPicture(String weiboId, String picId) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return false;
		String key = "weibo:"+weiboId + ":image";
		jedis.lpush(key, picId);		
		return true;
	}

	@Override
	public boolean deleteWeiboPicture(String weiboId, String picId) {
		String key = "weibo:"+weiboId + ":image";
		Long a=jedis.lrem(key, 1, picId);
		if(a==1)
		return true;
		return false;
	}

	@Override
	public List<String> getLikeList(String weiboId, int pageIndex,
			int numberPerPage) {
		String key = "weibo:"+weiboId + ":like";
		List<String> list = jedis.lrange(key, 0, -1);
		int start=pageIndex*numberPerPage;
	    int end=start+numberPerPage;
	    if(end>=list.size())
	    	end=list.size();
	    if(pageIndex<0||numberPerPage<=0||end>=list.size()){
	    	return null;
	    }
		return list.subList(start, end);
	
	}

	@Override
	public boolean deleteCommentFromWeibo(String fromWeiboId, String commentId) {
		String key = "weibo:"+fromWeiboId+ ":comment";
		Long a=jedis.lrem(key, 1, commentId);
		if(a==1){
		key = "weibo:"+fromWeiboId+ ":commentNumber";
		jedis.decr(key);
		return true;}
		return false;
	}


	@Override
	public List<String> getWeiboPicurl(String weiboId) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return null;
		return jedis.lrange("weibo:"+weiboId+":image", 0, -1);
	}


	@Override
	public boolean  insertLikeList(String weiboId, String userId) {		
		String key = "weibo:"+weiboId + ":like";
		List<String>list=jedis.lrange("weiboList",0,-1);
		for(int i=0;i<list.size();i++){
			if((list.get(i)).equals(weiboId))
			{
				List<String>list2=jedis.lrange(key,0,-1);
				for(int j=0;j<list2.size();j++){
					if(list2.get(j).equals(userId))
						return false;
				}
				jedis.lpush(key, userId);
				key = "weibo:"+weiboId + ":likeNumber";
				jedis.incr(key);
				return true;
			}
		}
	return false;

	}


//the userId means commentId ,the interface is wrong but i can't change it.
	@Override
	public boolean insertCommentList(String weiboId, String userId) {
		String key = "weibo:"+weiboId + ":comment";
		List<String>list=jedis.lrange("weiboList",0,-1);
		for(int i=0;i<list.size();i++){
			if((list.get(i)).equals(weiboId)){			
		jedis.lpush(key, userId);
		 key = "weibo:"+weiboId + ":commentNumber";
			jedis.incr(key);
		return true;}}
		return false;
	}

//the userId means fordwardweiboId ,the interface is wrong but i can't change it.
	@Override
	public boolean insertForwardList(String weiboId, String userId) {
		String key = "weibo:"+weiboId + ":forward";
		List<String>list=jedis.lrange("weiboList",0,-1);
		for(int i=0;i<list.size();i++){
			if((list.get(i)).equals(weiboId)){
		jedis.lpush(key, userId);
 key = "weibo:"+weiboId + ":forwardNumber";
		jedis.incr(key);
		return true;
			}}
		return false;
	}

	@Override
	public int getTotalWeiboNumber() {

		return Integer.valueOf(jedis.get("weiboNumber")).intValue();	
	}

	@Override
	public List<String> getTotalWeibo() {
		return jedis.lrange("weiboList", 0, -1);
	}

	@Override
	public String insertNewWeibo(Weibo weibo) {
		Long count=jedis.incr("weiboNumber");		
		String str = String.format("%05d", count);
		String key = "weibo:w" + str + ":";
		jedis.set(key + "owner", weibo.getUserId());
		jedis.set(key + "content", weibo.getContent());
		jedis.set(key + "sendTime", weibo.getDate());
		for(int i=0;i<weibo.getAtUserIdList().size();i++){
			jedis.lpush(key+"atUserList", weibo.getAtUserIdList().get(i));
		}
		for(int i=0;i<weibo.getTopicIdList().size();i++){
			jedis.lpush(key+"topicList", weibo.getTopicIdList().get(i));
		}
		jedis.set(key + "commentNumber","0");
		jedis.set(key + "forwardNumber","0");
		jedis.set(key + "likeNumber","0");
		jedis.lpush("weiboList", "w"+str);
		return "OK";
	}

	@Override
	public List<String> getAtUserList(String weiboId) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return null;
		String key="weibo:"+weiboId+":atUserList";		
		return jedis.lrange(key, 0, -1);
	}

	@Override
	public List<String> getTopicList(String weiboId) {
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return null;
		String key="weibo:"+weiboId+":topicList";		
		return jedis.lrange(key, 0, -1);
	}

	@Override
	public Weibo getWeibo(String weiboId) {
		Weibo weibo=new Weibo();
		List<String>list=jedis.lrange("weiboList",0,-1);
		boolean flag=false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(weiboId))
				flag=true;
		}
		if(flag==false)
			return null;
		String key = "weibo:" + weiboId;				
		weibo.setAtUserIdList(jedis.lrange(key+":atUserList", 0, -1));
		weibo.setCommentNumber(Integer.valueOf(jedis.get(key+":commentNumber")).intValue());
		weibo.setContent(jedis.get(key+":content"));
		weibo.setDate(jedis.get(key+":sendTime"));
		weibo.setForwardNumber(Integer.valueOf(jedis.get(key+":forwardNumber")).intValue());
		weibo.setLike(Integer.valueOf(jedis.get(key+":likeNumber")).intValue());
		weibo.setTopicIdList(jedis.lrange(key+":topicList", 0, -1));
		weibo.setUserId(jedis.get(key+":owner"));
		return weibo;
	}



	@Override
	public boolean deleteLikeList(String weiboId, String userId) {
		String key="weibo:"+weiboId+":like";
		Long a=jedis.lrem(key, 1, userId);
		if(a==1)
		{jedis.decr("weibo:"+weiboId+":likeNumber");
		return true;	
		}
		return false;
	}



	@Override
	public boolean deleteCommentList(String weiboId, String commentId) {
		String key="weibo:"+weiboId+":comment";
		Long a=jedis.lrem(key, 1, commentId);
		if(a==1)
		{jedis.decr("weibo:"+weiboId+":commentNumber");
		return true;	
		}
		return false;
	}


	@Override
	public boolean deleteForwardList(String weiboId, String forwardWeiboId) {
		String key="weibo:"+weiboId+":forward";
		Long a=jedis.lrem(key, 1, forwardWeiboId);
		if(a==1)
		{jedis.decr("weibo:"+weiboId+":forwardNumber");
		return true;	
		}
		return false;
	}

	

}
