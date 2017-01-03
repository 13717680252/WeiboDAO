package cn.edu.bjtu.weibo.dao;

import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;

import org.springframework.stereotype.Repository;

import java.util.*;

import junit.framework.TestCase;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import pl.quaternion.SentinelBasedJedisPoolWrapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

@Repository("keywordDAO")
public class KeyWordDAOImpl implements KeyWordDAO {
Jedis jedis;
	
	
	public KeyWordDAOImpl() {
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
	public boolean insert(String word) {
		String key="keyword:keyword";
		List<String>list=jedis.lrange(key, 0, -1);
		for(int i=0;i<list.size();i++){
			if(word==list.get(i))
				return false;
		}
		jedis.lpush(key, word);
		return true;
	}

	@Override
	public List<String> getAll() {
		String key="keyword:keyword";
		return jedis.lrange(key,0,-1);
		
	}

	@Override
	public boolean delete(String word) {
		String key="keyword:keyword";
		long count=jedis.lrem(key, 1, word);
		if(count>=1)
		return true;
		else return false;
	}

}
