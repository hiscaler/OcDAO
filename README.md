# OcDAO
OpenCart Database Access Objects

## 使用方法

```php
$this->load->library('OcDao');
$ocDao = new OcDao($this->registry);
$ocDao->insert('user', ['username' => "hiscaler"])->execute();
$ocDao->update('user', ['username' => 'John'], ['id' => 1])->execute();
```

