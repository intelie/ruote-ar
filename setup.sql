
CREATE SCHEMA IF NOT EXISTS ruote_ar_test;

USE ruote_ar_test;

DROP TABLE IF EXISTS documents;

CREATE TABLE `documents` (
  `ide` varchar(255) NOT NULL,
  `rev` int(11) NOT NULL,
  `typ` varchar(255) NOT NULL,
  `doc` mediumtext NOT NULL,
  `wfid` varchar(255) DEFAULT NULL,
  `participant_name` varchar(512) DEFAULT NULL,
  `worker` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`typ`,`ide`,`rev`),
  KEY `index_documents_on_wfid` (`wfid`)
) ENGINE=InnoDB CHARSET=utf8;
