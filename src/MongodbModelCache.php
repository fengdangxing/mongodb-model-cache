<?php
declare(strict_types=1);

namespace Fengdangxing\MongodbModelCache;

use Fengdangxing\HyperfRedis\RedisHelper;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Database\Model\Builder;
use Hyperf\Utils\Codec\Json;
use GiocoPlus\Mongodb\MongoDb;
use Hyperf\Di\Annotation\Inject;

class MongodbModelCache
{
    /**
     * @Inject
     * @var ConfigInterface
     */
    protected $config;

    /**
     * @Inject()
     * @var MongoDb
     */
    protected $mongoDbClient;

    protected $mongoConn;

    const  REDIS_LIST = "mongo:list:";
    const  REDIS_INFO = "mongo:info:";

    public function __construct()
    {
        $this->config->set("mongodb.dbYB", config('mongodb.default')); # 前綴 "mongodb."
        $this->mongoConn = $this->mongoDbClient->setPool("dbYB");
    }

    /**
     * @Notes:插入一行
     * @param array $data
     * @return int
     */
    public static function insertOneGetId(string $collName, array $data): bool
    {
        $conn = (new MongodbModelCache())->mongoConn;
        $result = $conn->insert($collName, $data);
        self::delRedis(true);
        return $result;
    }

    /**
     * @Notes:插入多行
     * @param array $data | [
     * ['email' => 'taylor@example.com', 'votes' => 0],
     * ['email' => 'dayle@example.com', 'votes' => 0]
     * ]
     * @return bool
     */
    public static function insertMore(string $collName, array $data): bool
    {
        $conn = (new MongodbModelCache())->mongoConn;
        $result = $conn->insertAll($collName, $data);
        self::delRedis(true);
        return $result;
    }

    /**
     * @Notes: 修改信息
     * @param array $where
     * @param array $update ['field'=>'value','field'=>'value','field'=>'value']
     * @return int
     */
    public static function updateInfo(string $collName, array $filter = [], array $update = []): int
    {
        $conn = (new MongodbModelCache())->mongoConn;
        $result = $conn->updateRow($collName, $filter, $update);
        self::delRedis(true);
        return $result;
    }

    /**
     * @Notes: 删除
     * @return
     */
    public static function softDelete(string $collName, array $filter, $limit = false): bool
    {
        $conn = (new MongodbModelCache())->mongoConn;
        $result = $conn->delete($collName, $filter, $limit);
        self::delRedis(true);
        return $result;
    }

    /**
     * @Notes: 获取一行数据
     * @param string $collName
     * @param array $field
     * @param array $options = [
     * 'skip' => $skip,
     * 'limit' => $limit,
     * 'sort' => ['_id' => 1],
     * ];
     * @return array
     */
    public static function getRow(string $collName, array $filter, array $options, bool $cache = true): array
    {
        $options['limit'] = 1;
        return self::getRedis(static::$redisInfo, __FUNCTION__, $params, $where, $cache, $builder);
    }

    /**
     * @Notes: 获取总数
     * @param array $where
     * @param string[] $field
     * @param bool $cache
     * @return int
     */
    public static function getCount(string $collName, array $filter, bool $cache = true): int
    {
        return self::getRedis(static::$redisInfo, __FUNCTION__, $collName, $filter, [], 0, 0, $cache);
    }

    /**
     * @Notes:列表
     * @param string $collName
     * @param array $filter
     * @param array $options = [
     * 'skip' => $skip,
     * 'limit' => $limit,
     * 'sort' => ['_id' => 1],
     * ];
     * @return array
     */
    public static function getPageList(string $collName, array $filter, array $options, int $page = 1, int $limit = 10)
    {
        return self::getRedis(static::$redisList, __FUNCTION__, $collName, $filter, $options, $limit, $page, true);
    }

    /**
     * @Notes: 获取所有数据
     * @param string $collName
     * @param array $filter
     * @param array $options = [
     * 'skip' => $skip,
     * 'limit' => $limit,
     * 'sort' => ['_id' => 1],
     * ];
     * @param bool $cache
     * @return array
     */
    public static function getAllList(string $collName, array $filter, array $options, bool $cache = true)
    {
        return self::getRedis(static::$redisList, __FUNCTION__, $collName, $filter, $options, 0, 0, $cache);
    }

    /**
     * @Notes: 命令
     * @Author: fengdangxing
     * @param string $collName
     * @param array $cmd = [
     * 'aggregate' => $collName,
     * 'pipeline' => [
     * ['$match' => $filter],
     * ['$group' => $group]
     * ],
     * 'cursor' => ['batchSize' => 0]
     * ];
     * @Date: 2024/7/29 14:48
     * @return void
     */
    public static function command(string $collName, array $cmd)
    {
        return self::getRedis(static::$redisList, __FUNCTION__, $collName, $cmd, [], 0, 0, $cache);
    }

    public static function delRedis($isTrue = false)
    {
        RedisHelper::init()->del(static::$redisList);
        RedisHelper::init()->del(static::$redisInfo);
        if ($isTrue) {
            call_user_func("static::hasDelRedis");
        }
    }

    /**
     * @Notes: 根据模型列表删除对应缓存， 用于事务后删除缓存避免错误
     * @param $models //删除缓存 [PublishBatchDetailModel::class, PublishPageRecord::class, PublishBatchModel::class,PublishAuditModel::class]);
     * @return void
     */
    public static function delRedisCacheByTransaction($models)
    {
        foreach ($models as $model) {
            $model::delRedis(true);
        }
    }

    private static function getRedis(string $key, string $mod, string $collName, $filter, $options, $limit, $page, $cache)
    {
        $ret = [];

        $hKey = md5($collName . Json::encode($options) . Json::encode($filter) . $mod);
        if (RedisHelper::init()->hExists($key, $hKey) && $cache) {
            $retJson = RedisHelper::init()->hGet($key, $hKey);
            $ret = Json::decode($retJson, true);
        } else {
            $conn = (new MongodbModelCache())->mongoConn;
            switch ($mod) {
                case 'getAllList':
                    $ret = $conn->fetchAll($collName, $filter, $options);
                    break;
                case 'getPageList':
                    $pageSize = ($page - 1) * $limit;
                    $ret = $conn->fetchPagination($collName, $limit, $pageSize, $filter, $options);
                    break;
                case 'command':
                    $ret = $conn->command($collName, $filter);
                    break;
                case 'getRow':
                    $ret = $conn->fetchAll($collName, $filter, $options);
                    break;
                case 'getCount':
                    $ret = $conn->count($collName, $filter);
                    break;
                default:
                    break;

            }
            RedisHelper::init()->hSet($key, $hKey, is_array($ret) ? Json::encode($ret) : $ret);
            RedisHelper::init()->expire($key, RedisHelper::$timeout);
        }
        return $ret;
    }
}
