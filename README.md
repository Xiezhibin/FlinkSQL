# FlinkSQL (本地环境运行）

执行步骤：
- IDEA + kafka_2.12-2.4.1 + flink-1.13.2
- 启动kafka 、 zookeeper、 flink 
```
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
bin/kafka-server-start.sh -daemon config/server.properties
bin/start-cluster.sh
```
- 创建topic userbehavior1 并写入指定json数据
```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --partitions 1 --topic userbehavior1
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic userbehavior1
{"id": 1, "site": "www.baidu.com", "proctime": "2020-04-11 00:00:01"}
{"id": 2, "site": "www.bilibili.com/", "proctime": "2020-04-11 00:00:02"}
{"id": 3, "site": "www.baidu.com", "proctime": "2020-04-11 00:00:03"}
{"id": 4, "site": "www.baidu.com/", "proctime": "2020-04-11 00:00:05"}
{"id": 5, "site": "www.baidu.com", "proctime": "2020-04-11 00:00:06"}
{"id": 6, "site": "www.bilibili.com/", "proctime": "2020-04-11 00:00:07"}
{"id": 7, "site": "https://github.com/tygxy", "proctime": "2020-04-11 00:00:08"}
{"id": 8, "site": "www.bilibili.com/", "proctime": "2020-04-11 00:00:09"}
{"id": 9, "site": "www.baidu.com", "proctime": "2020-04-11 00:00:11"}
{"id": 10, "site": "www.bilibili.com/", "proctime": "2020-04-11 00:00:18"}
```
- IDEA 编译后执行主函数
- 查看消费者是否产生 
```
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic userbehavior2 --from-beginning
```
## 运行过程中遇到的问题
- 主线程运行时候报异常错误
maven依赖并未打包进入，导入通过删除<scope>provided</scope> 中存在的解决了这个问题。
```
Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.
```
- FactoryUtil discoverFactory 报错，It is the responsibility of each factory to perform validation before returning an instance.
```
Exception in thread "main" org.apache.flink.table.api.ValidationException: Unable to create a source for reading table 'default_catalog.default_database.kafkaTable1'.

Table options are:

'connector'='kafka'
'format'='json'
'properties.bootstrap.servers'='localhost:9092'
'properties.group.id'='testGroup'
'scan.startup.mode'='earliest-offset'
'topic'='userbehavior1'
	at org.apache.flink.table.factories.FactoryUtil.createTableSource(FactoryUtil.java:137)
	at org.apache.flink.table.planner.plan.schema.CatalogSourceTable.createDynamicTableSource(CatalogSourceTable.java:116)
	at org.apache.flink.table.planner.plan.schema.CatalogSourceTable.toRel(CatalogSourceTable.java:82)
	at org.apache.calcite.sql2rel.SqlToRelConverter.toRel(SqlToRelConverter.java:3585)
	at org.apache.calcite.sql2rel.SqlToRelConverter.convertIdentifier(SqlToRelConverter.java:2507)
	at org.apache.calcite.sql2rel.SqlToRelConverter.convertFrom(SqlToRelConverter.java:2144)
	at org.apache.calcite.sql2rel.SqlToRelConverter.convertFrom(SqlToRelConverter.java:2093)
	at org.apache.calcite.sql2rel.SqlToRelConverter.convertFrom(SqlToRelConverter.java:2050)
	at org.apache.calcite.sql2rel.SqlToRelConverter.convertSelectImpl(SqlToRelConverter.java:663)
	at org.apache.calcite.sql2rel.SqlToRelConverter.convertSelect(SqlToRelConverter.java:644)
	at org.apache.calcite.sql2rel.SqlToRelConverter.convertQueryRecursive(SqlToRelConverter.java:3438)
	at org.apache.calcite.sql2rel.SqlToRelConverter.convertQuery(SqlToRelConverter.java:570)
	at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.org$apache$flink$table$planner$calcite$FlinkPlannerImpl$$rel(FlinkPlannerImpl.scala:170)
	at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.rel(FlinkPlannerImpl.scala:162)
	at org.apache.flink.table.planner.operations.SqlToOperationConverter.toQueryOperation(SqlToOperationConverter.java:967)
	at org.apache.flink.table.planner.operations.SqlToOperationConverter.convertSqlQuery(SqlToOperationConverter.java:936)
	at org.apache.flink.table.planner.operations.SqlToOperationConverter.convert(SqlToOperationConverter.java:275)
	at org.apache.flink.table.planner.operations.SqlToOperationConverter.convertSqlInsert(SqlToOperationConverter.java:595)
	at org.apache.flink.table.planner.operations.SqlToOperationConverter.convert(SqlToOperationConverter.java:268)
	at org.apache.flink.table.planner.delegation.ParserImpl.parse(ParserImpl.java:101)
	at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeSql(TableEnvironmentImpl.java:724)
	at testFlink.SQLStreaming.main(SQLStreaming.java:75)
Caused by: org.apache.flink.table.api.ValidationException: Could not find any factory for identifier 'json' that implements 'org.apache.flink.table.factories.DeserializationFormatFactory' in the classpath.

raw
	at org.apache.flink.table.factories.FactoryUtil.discoverFactory(FactoryUtil.java:319)
	at org.apache.flink.table.factories.FactoryUtil$TableFactoryHelper.discoverOptionalFormatFactory(FactoryUtil.java:751)
	at org.apache.flink.table.factories.FactoryUtil$TableFactoryHelper.discoverOptionalDecodingFormat(FactoryUtil.java:649)
	at org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory.getValueDecodingFormat(KafkaDynamicTableFactory.java:275)
	at org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory.createDynamicTableSource(KafkaDynamicTableFactory.java:142)
	at org.apache.flink.table.factories.FactoryUtil.createTableSource(FactoryUtil.java:134)
	... 21 more
```

需要，提前引入flink-json依赖。

```
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-json</artifactId>
    <version>${flink.version}</version>
</dependency>
```
- SQL中变量名不规范
变量名称使用`var`, with中参数使用'parameters'.

- json输入不规范
需要加入{}.


## 查看结果
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic userbehavior2 --from-beginning


