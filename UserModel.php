<?php

namespace Fengdangxing\MongodbModelCache;

use GiocoPlus\Mongodb\MongoDb;

/**
 * @Notes: 事列
 * @Author: fengdangxing
 *
 * @Date: 2024/7/29 14:00
 */
class UserModel extends MongodbModelCache
{
    public function add()
    {
        return self::insertOneGetId('test', ['id' => 1]);
    }
}