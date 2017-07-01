# mobike-crawler

Get the position of mobikes in a rectangular range.

---

FullSHDivide into 28 pieces = 400s/round

MyLinux 	= 14 pieces 	= 1 ~ 14    = localhost.mobike

Server 		= 8 pieces	    = 15 ~ 22   = MyLinux.mobike

T420s		= 6 pieces	    = 23 ~ 28   = localhost.mobike


2017-06-30 11:40am +

---

Should add:
1. MongoDB-JSON to MySQL-SQL

---


Mysql Environment:

    -- ----------------------------
    -- Table structure for mobike_seed
    -- ----------------------------
    DROP TABLE IF EXISTS `mobike_seed`;
    CREATE TABLE `mobike_seed` (
      `time` datetime DEFAULT NULL,
      `bikeid` varchar(20) DEFAULT NULL,
      `biketype` smallint(6) DEFAULT NULL,
      `distid` varchar(20) DEFAULT NULL,
      `distnum` tinyint(4) DEFAULT NULL,
      `type` tinyint(4) DEFAULT NULL,
      `x` double(10,6) DEFAULT NULL,
      `y` double(10,6) DEFAULT NULL,
      `host` tinyint(4) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    
    -- ----------------------------
    -- Table structure for mobike_log
    -- ----------------------------
    DROP TABLE IF EXISTS `mobike_log`;
    CREATE TABLE `mobike_log` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `start` datetime DEFAULT NULL,
      `end` datetime DEFAULT NULL,
      `left` double(10,6) DEFAULT NULL,
      `right` double(10,6) DEFAULT NULL,
      `top` double(10,6) DEFAULT NULL,
      `bottom` double(10,6) DEFAULT NULL,
      `cost` double DEFAULT NULL,
      `host` tinyint(4) DEFAULT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=37 DEFAULT CHARSET=utf8;
    
    -- ----------------------------
    -- Table structure for mobike_error
    -- ----------------------------
    DROP TABLE IF EXISTS `mobike_error`;
    CREATE TABLE `mobike_error` (
      `id` int(11) NOT NULL,
      `time` datetime DEFAULT NULL,
      `lon` double(10,6) DEFAULT NULL,
      `lat` double(10,6) DEFAULT NULL,
      `host` tinyint(4) DEFAULT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
