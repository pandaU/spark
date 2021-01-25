package com.spark.scala

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisPoolUtil {
  val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  //10000代表超时时间（10秒）
  val pool = new JedisPool(config, "redis", 6379)

  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn = RedisPoolUtil.getConnection()
    conn.set("ds","1")
  }
}
