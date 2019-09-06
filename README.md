# OcDAO
OpenCart Database Access Objects

## 使用方法

### 加载
```php
$this->load->library('OcDao');
$ocDao = new OcDao($this->registry, false);
```

如果您需要启用调试模式，在创建 OcDao 对象的时候，第二个参数请传入 true, 之后每一次执行 SQL 语句，都将打印当前执行的 SQL 语句以及调用文件和行数，方便您调试。生产环境下，请关闭。以免暴露相关信息。

```php
...
$ocDao = new OcDao($this->registry, true);
```


### 添加
```php
$ocDao->reset()->insert('user', ['username' => "hiscaler"])->execute();
```

### 更新
```php
$ocDao->reset()->update('user', ['username' => 'John'], ['id' => 1])->execute();
```

### 查询

#### 查询一条数据
```php
$user = $this->ocDao
            ->reset()
            ->from('{{%user}}')
            ->select(['user_id', 'username'])
            ->orderBy(['user_id' => SORT_DESC])
            ->one();
```
`one()` 函数如果返回 false 表示没有查询到相应的数据，否则的话返回一个键值对数组，键为字段名，值为字段保存值。

生成的 SQL 如下:
```sql
SELECT `user_id`, `username` FROM `oc_user` ORDER BY `user_id` DESC LIMIT 0, 1
```

#### 查询多条数据
```php
$users = $this->ocDao
            ->reset()
            ->from('{{%user}}')
            ->select(['user_id', 'username'])
            ->limit(2)
            ->orderBy(['user_id' => SORT_DESC])
            ->all();
```
`all()` 总是返回一个数组，无论是否成功，如果返回的是空数组则表示没有查询到您要的数据。

生成的 SQL 如下:
```sql
SELECT `user_id`, `username` FROM `oc_user` ORDER BY `user_id` DESC LIMIT 0, 2
```
