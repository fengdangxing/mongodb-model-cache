# hyperf-mongodb-model-cache
hyperf-mongodb 模型缓存、查询mongodb后自动缓存
#配置mongodb  config/autoload/mongodb.php
```php
return [
    'default' => [
        'username' => env('MONGODB_USERNAME', ''),
        'password' => env('MONGODB_PASSWORD', ''),
        'host' => explode(',', env('MONGODB_HOST', '127.0.0.1')),
        'port' => env('MONGODB_PORT', 27017),
        'db' => env('MONGODB_DB', 'test'),
        'options'  => [
            'database' => env('MONGODB_DB', 'test'),
            'authSource' => env('MONGODB_DB', 'test'),
            // 需要配置 username
            // 'authMechanism' => env('MONGODB_AuthMechanism', 'SCRAM-SHA-256'),
            //设置复制集,没有不设置
            'replica' => env('MONGODB_REPLICA', 'rs0'),
            'readPreference' => env('MONGODB_READPREFERENCE', 'primary')
        ],
        'pool' => [
            'min_connections' => 1,
            'max_connections' => 10000,
            'connect_timeout' => 10.0,
            'wait_timeout' => 3.0,
            'heartbeat' => -1,
            'max_idle_time' => (float)env('MONGODB_MAX_IDLE_TIME', 60),
        ],
    ],
];

```
#配置redis config/autoload/redis.php
```php
<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */
return [
    'default' => [
        'host' => env('REDIS_HOST', 'localhost'),
        'auth' => env('REDIS_AUTH', null),
        'port' => (int) env('REDIS_PORT', 6379),
        'db' => (int) env('REDIS_DB', 0),
        'pool' => [
            'min_connections' => 1,
            'max_connections' => 50,
            'connect_timeout' => 10.0,
            'wait_timeout' => 3.0,
            'heartbeat' => -1,
            'max_idle_time' => (float) env('REDIS_MAX_IDLE_TIME', 60),
        ],
    ]
];
```
#使用
```php
<?php

namespace Fengdangxing\MongodbModelCache;

use GiocoPlus\Mongodb\MongoDb;

/**
 * @Notes: 事列
 * @Author: fengdangxing
 * @Date: 2024/7/29 14:00
 */
class UserModel extends MongodbModelCache
{
    //回调
    public function hasMongodbDelRedis($collName){
        //UserModel::delRedis($collName);
    }
   
    public function add()
    {
        return self::insertOneGetId('test', ['id' => 1]);
    }
}
```

