<?php

namespace Fengdangxing\Aes;

use Hyperf\Contract\ConfigInterface;
use Hyperf\Utils\ApplicationContext;
use HyperfExt\Encryption\Crypt;

class Aes
{
    /**
     * 加密
     * @param string $data
     * @return string
     */
    public static function encrypt(string $data)
    {
        $config = ApplicationContext::getContainer()->get(ConfigInterface::class);
        return openssl_encrypt($json, $config['app.fengdangxing.encrypt.method'], $config['app.fengdangxing.encrypt.key'], OPENSSL_RAW_DATA, $config['app.fengdangxing.encrypt.iv']);
    }

    /**
     * 解密
     * @param string $data
     * @return mixed
     */
    public static function decrypt(string $data)
    {
        return openssl_decrypt($string, $config['app.fengdangxing.encrypt.method'], $config['app.fengdangxing.encrypt.key'], OPENSSL_RAW_DATA, $config['app.fengdangxing.encrypt.iv']);
    }


}