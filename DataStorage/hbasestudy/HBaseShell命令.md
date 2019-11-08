## HBase Shell命令

### 1. 名称命令表达式
|指令说明       |表达式  
|:---   |:---
|查看存在哪些表	|list
|创建表	|create '表名称', '列名称1','列名称2','列名称N'
|添加记录 |put '表名称', '行名称', '列名称:', '值'
|查看记录 |get '表名称', '行名称'
|查看表中的记录总数	|count '表名称'
|删除记录	|delete '表名' ,'行名称' , '列名称'
|删除一张表	|先要屏蔽该表，才能对该表进行删除，第一步 disable '表名称' 第二步 drop '表名称'
|查看所有记录	|scan "表名称"
|查看某个表某个列中所有数据 |scan "表名称" , ['列名称:']
|更新记录	|就是重写一遍进行覆盖

### 2. 一般操作
> #### 2.1. HBase shell中的帮助命令非常强大，使用help获得全部命令的列表，使用help ‘command_name’获得某一个命令的详细信息。 例如：
```
help 'list'
```

> #### 2.2. 查询服务器状态
```
status
```

> #### 2.3. 查询Hbase版本
```
version
```

> #### 2.4. 查看所有表
```
list
```

### 3. 增删改
> #### 3.1 创建一个表
```
create 'member','member_id','address','info’  
```
> #### 3.2 获得表的描述
```
describe 'member'
```

> #### 3.3 添加一个列族
```
alter 'member', 'id'
```

> #### 3.4 删除一个列族
```
alter 'member', {NAME => 'member_id', METHOD => 'delete’}
```

> #### 3.5 删除列

> 1）通过delete命令，我们可以删除id为某个值的‘info:age’字段，接下来的get就无视了
```
delete 'member','debugo','info:age'
get 'member','debugo','info:age'
```
> 2）删除整行的值：deleteall
```
deleteall 'member','debugo'
get 'member',’debugo'
```

> #### 3.6 通过enable和disable来启用/禁用这个表,相应的可以通过is_enabled和is_disabled来检查表是否被禁用。
```
is_enabled 'member'
is_disabled 'member'
```

> #### 3.7 使用exists来检查表是否存在
```
exists 'member'
```

> #### 3.8 删除表需要先将表disable。
```
  disable 'member'
  drop 'member'
```

> #### 3.9 put

在HBase shell中，我们可以通过put命令来插入数据。例如我们新创建一个表，它拥有id、address和info三个列簇，并插入一些数据。列簇下的列不需要提前创建，在需要时通过:来指定即可。
```
create 'member','id','address','info'
put 'member', 'debugo','id','11'
put 'member', 'debugo','info:age','27'
put 'member', 'debugo','info:birthday','1987-04-04'
put 'member', 'debugo','info:industry', 'it'
put 'member', 'debugo','address:city','beijing'
put 'member', 'debugo','address:country','china'
put 'member', 'Sariel', 'id', '21'
put 'member', 'Sariel','info:age', '26'
put 'member', 'Sariel','info:birthday', '1988-05-09 '
put 'member', 'Sariel','info:industry', 'it'
put 'member', 'Sariel','address:city', 'beijing'
put 'member', 'Sariel','address:country', 'china'
put 'member', 'Elvis', 'id', '22'
put 'member', 'Elvis','info:age', '26'
put 'member', 'Elvis','info:birthday', '1988-09-14 '
put 'member', 'Elvis','info:industry', 'it'
put 'member', 'Elvis','address:city', 'beijing'
put 'member', 'Elvis','address:country', 'china'
```

### 4. 查询
> #### 4.1 查询表中有多少行：count
```
count 'member'
```

> #### 4.2 get
> 1) 获取一个id的所有数据 
```
get 'member', ‘Sariel'
```

> 2) 获得一个id，一个列簇（一个列）中的所有数据:
```
get 'member', 'Sariel', 'info'
```

> #### 4.3 查询整表数据
```
scan 'member'
```

> #### 4.4 扫描整个列簇
```
scan 'member', {COLUMN=>'info'}
```

> #### 4.5 指定扫描其中的某个列：
```
scan 'member', {COLUMNS=> 'info:birthday'}
```

> #### 4.6 除了列（COLUMNS）修饰词外，HBase还支持Limit（限制查询结果行数），STARTROW（ROWKEY起始行。会先根据这个key定位到region，再向后扫描）、STOPROW(结束行)、TIMERANGE（限定时间戳范围）、VERSIONS（版本数）、和FILTER（按条件过滤行）等。比如我们从Sariel这个rowkey开始，找下一个行的最新版本
```
scan 'member', { STARTROW => 'Sariel', LIMIT=>1, VERSIONS=>1}
```

> #### 4.7 Filter是一个非常强大的修饰词，可以设定一系列条件来进行过滤。比如我们要限制某个列的值等于26
```
scan 'member', FILTER=>"ValueFilter(=,'binary:26’)"
```
值包含6这个值：
```
scan 'member', FILTER=>"ValueFilter(=,'substring:6')"
```
列名中的前缀为birthday的
```
scan 'member', FILTER=>"ColumnPrefixFilter('birth')"
```
FILTER中支持多个过滤条件通过括号、AND和OR的条件组合
```
scan 'member', FILTER=>"ColumnPrefixFilter('birth') AND ValueFilter ValueFilter(=,'substring:1988')"
```
PrefixFilter是对Rowkey的前缀进行判断,这是一个非常常用的功能。
```
scan 'member', FILTER=>"PrefixFilter('E')"
```
