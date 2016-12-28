package cn.edu.bjtu.weibo.dao;

import java.util.List;

import redis.clients.jedis.Jedis;
@Respository(keywordDAO)
public class KeyWordDAOImpl implements KeyWordDAO {
Jedis jedis;
	
	
	public KeyWordDAOImpl() {
		jedis = new Jedis("localhost", 6379);
	}
	@Override
	public boolean insert(String word) {
		String key="keyword:keyword";
		jedis.lpushx(key, word);
		return false;
	}

	@Override
	public List<String> getAll() {
		String key="keyword:keyword";
		jedis.lrange(key,0,-1);
		return null;
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
