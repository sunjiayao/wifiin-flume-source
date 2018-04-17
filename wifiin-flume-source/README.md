
## flume 读取指定目录下文件的 source

### 相关配置

|属性|详细|默认值|
| ------------- |:-------------:|:-----|
|file|文件名(正则表达式)|^yyyy-MM-dd.log$|
|path|文件所在目录|/|
|offset_path|offset 存储路径||
|event_size|每次提交事件条数|1000|
|file_lines_size|文件缓存行数|100_000|
|charset|事件编码|UTF8|
