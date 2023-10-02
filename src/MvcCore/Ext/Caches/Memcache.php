<?php

/**
 * MvcCore
 *
 * This source file is subject to the BSD 3 License
 * For the full copyright and license information, please view
 * the LICENSE.md file that are distributed with this source code.
 *
 * @copyright	Copyright (c) 2016 Tom Flidr (https://github.com/mvccore)
 * @license		https://mvccore.github.io/docs/mvccore/5.0.0/LICENSE.md
 */

namespace MvcCore\Ext\Caches;

class Memcache implements \MvcCore\Ext\ICache {
	
	/**
	 * MvcCore Extension - Cache - Memcached - version:
	 * Comparison by PHP function version_compare();
	 * @see http://php.net/manual/en/function.version-compare.php
	 */
	const VERSION = '5.2.0';

	/** @var array<string,\MvcCore\Ext\Caches\Memcached> */
	protected static $instances	= [];

	/** @var array */
	protected static $defaults	= [
		\MvcCore\Ext\ICache::CONNECTION_PERSISTENCE	=> 'default',
		\MvcCore\Ext\ICache::CONNECTION_NAME		=> NULL,
		\MvcCore\Ext\ICache::CONNECTION_HOST		=> '127.0.0.1',
		\MvcCore\Ext\ICache::CONNECTION_PORT		=> 11211,
		\MvcCore\Ext\ICache::CONNECTION_TIMEOUT		=> 500, // in milliseconds, 0.5s, only for non-blocking I/O
		\MvcCore\Ext\ICache::PROVIDER_CONFIG		=> [
			//'\Memcached::OPT_SERIALIZER'			=> '\Memcached::HAVE_IGBINARY', // resolved later in code
			'\Memcached::OPT_LIBKETAMA_COMPATIBLE'	=> TRUE,
			'\Memcached::OPT_POLL_TIMEOUT'			=> 500, // in milliseconds, 0.5s
			'\Memcached::OPT_SEND_TIMEOUT'			=> 1000000, // in microseconds, 0.01s
			'\Memcached::OPT_RECV_TIMEOUT'			=> 1000000, // in microseconds, 0.01s
			'\Memcached::OPT_COMPRESSION'			=> FALSE,
			'\Memcached::OPT_SERVER_FAILURE_LIMIT'	=> 5,
			'\Memcached::OPT_REMOVE_FAILED_SERVERS'	=> TRUE,
		]
	];

	/** @var \stdClass|NULL */
	protected $config			= NULL;
	
	/** @var bool|NULL */
	protected $memcachedExists	= NULL;

	/** @var \Memcached|NULL */
	protected $memcached		= NULL;

	/** @var bool */
	protected $enabled			= FALSE;

	/** @var bool|NULL */
	protected $connected		= NULL;

	/** @var \MvcCore\Application */
	protected $application		= NULL;

	/**
	 * @inheritDoc
	 * @param string|array|NULL $connectionArguments...
	 * If string, it's used as connection name.
	 * If array, it's used as connection config array with keys:
	 *  - `name`		default: 'default'
	 *  - `host`		default: '127.0.0.1',
	 *                  it could be single IP string or an array of IPs and weights for multiple servers:
	 *                  `['192.168.0.10' => 1, '192.168.0.11' => 2]`
	 *  - `port`		default: 11211,
	 *                  it could be single port integer for single or multiple servers 
	 *                  or an array of ports for multiple servers:
	 *                  `[11211, 11212]`
	 *  - `database`	default: $_SERVER['SERVER_NAME']
	 *  - `timeout`		default: NULL
	 *  - `provider`	default: []
	 *  If NULL, there is returned `default` connection
	 *  name with default initial configuration values.
	 * @return \MvcCore\Ext\Caches\Memcached
	 */
	public static function GetInstance (/*...$connectionNameOrArguments = NULL*/) {
		$args = func_get_args();
		$nameKey = self::CONNECTION_NAME;
		$config = static::$defaults;
		$connectionName = $config[$nameKey];
		if (isset($args[0])) {
			$arg = & $args[0];
			if (is_string($arg)) {
				$connectionName = $arg;
			} else if (is_array($arg)) {
				$connectionName = isset($arg[$nameKey])
					? $arg[$nameKey]
					: static::$defaults[$nameKey];
				$config = $arg;
			} else if ($arg !== NULL) {
				throw new \InvalidArgumentException(
					"[".get_class()."] Cache instance getter argument could be ".
					"only a string connection name or connection config array."
				);
			}
		}
		if (!isset(self::$instances[$connectionName]))
			self::$instances[$connectionName] = new static($config);
		return self::$instances[$connectionName];
	}

	/**
	 * @inheritDoc
	 * @param array $config Connection config array with keys:
	 *  - `name`		default: 'default'
	 *  - `host`		default: '127.0.0.1',
	 *                  it could be single IP string or an array of IPs and weights for multiple servers:
	 *                  `['192.168.0.10' => 1, '192.168.0.11' => 2]`
	 *  - `port`		default: 11211,
	 *                  it could be single port integer for single or multiple servers 
	 *                  or an array of ports for multiple servers:
	 *                  `[11211, 11212]`
	 *  - `database`	default: $_SERVER['SERVER_NAME']
	 *  - `timeout`		default: NULL
	 *  - `provider`	default: []
	 */
	protected function __construct (array $config = []) {
		$this->memcachedExists = class_exists('\Memcached');
		$hostKey	= self::CONNECTION_HOST;
		$portKey	= self::CONNECTION_PORT;
		$timeoutKey	= self::CONNECTION_TIMEOUT;
		$dbKey		= self::CONNECTION_DATABASE;
		if (!isset($config[$hostKey]))
			$config[$hostKey] = static::$defaults[$hostKey];
		if (!isset($config[$portKey]))
			$config[$portKey] = static::$defaults[$portKey];
		if (!isset($config[$timeoutKey])) 
			$config[$timeoutKey] = static::$defaults[$timeoutKey];
		if (!isset($config[$dbKey]))
			$config[$dbKey]	= static::$defaults[$dbKey];
		$this->config = (object) $config;
		$this->application = \MvcCore\Application::GetInstance();
	}

	/**
	 * @inheritDoc
	 * @return bool
	 */
	public function Connect () {
		if ($this->connected) {
			return TRUE;
		} else if (!$this->memcachedExists) {
			$this->enabled = FALSE;
			$this->connected = FALSE;
		} else {
			$toolClass = $this->application->GetToolClass();
			$persKey	= self::CONNECTION_PERSISTENCE;
			$timeoutKey = self::CONNECTION_TIMEOUT;
			$provKey	= self::PROVIDER_CONFIG;

			try {
				if (isset($this->config->{$persKey})) {
					$this->memcached = new \Memcached($this->config->{$persKey});
				} else {
					$this->memcached = new \Memcached();
				}
				if (
					count($this->memcached->getServerList()) > 0 && 
					$this->memcached->isPersistent()
				) {
					$this->connected = TRUE;
				} else {
					$this->connectConfigure();
					$this->connected = $this->connectExecute();
				}
				$this->enabled = $this->connected;
				if ($this->enabled)
					$this->memcached->setOption(
						\Memcached::OPT_PREFIX_KEY, 
						$this->config->{self::CONNECTION_DATABASE}.':'
					);

			} catch (\Exception $e1) { // backward compatibility
				$this->exceptionHandler($e1);
				$this->connected = FALSE;
				$this->enabled = FALSE;
			} catch (\Throwable $e2) {
				$this->exceptionHandler($e2);
				$this->connected = FALSE;
				$this->enabled = FALSE;
			}
		}
		return $this->connected;
	}
	
	/**
	 * Configure connection provider before connection is established.
	 * @return void
	 */
	protected function connectConfigure () {
		// configure provider options:
		$timeoutKey = self::CONNECTION_TIMEOUT;
		$provKey = self::PROVIDER_CONFIG;
		$provConfig = isset($this->config->{$provKey})
			? $this->config->{$provKey}
			: [];
		$provConfigDefault = static::$defaults[$provKey];
		$mcConstBegin = '\Memcached::';
		foreach ($provConfigDefault as $constStr => $rawValue) {
			$const = constant($constStr);
			if (!isset($provConfig[$const])) {
				if (is_string($rawValue) && strpos($rawValue, $mcConstBegin) === 0) {
					if (!defined($rawValue))
						continue;
					$value = constant($rawValue);
				} else {
					$value = $rawValue;
				}
				$provConfig[$const] = $value;
			}
		}
		if (!isset($provConfig[\Memcached::OPT_SERIALIZER]))
			$provConfig[\Memcached::OPT_SERIALIZER] = $this->memcached->getOption(\Memcached::HAVE_IGBINARY)
				? \Memcached::SERIALIZER_IGBINARY
				: \Memcached::SERIALIZER_PHP;
		$this->memcached->setOption(\Memcached::OPT_CONNECT_TIMEOUT, $this->config->{$timeoutKey});
		foreach ($provConfig as $provOptKey => $provOptVal)
			$this->memcached->setOption($provOptKey, $provOptVal);
		// configure servers:
		$hosts = [];
		$ports = [];
		$priorities = [];
		$cfgHost = $this->config->{self::CONNECTION_HOST};
		$cfgPort = $this->config->{self::CONNECTION_PORT};
		if (is_string($cfgHost)) {
			$hosts = [$cfgHost];
			$priorities = [1];
		} else if (is_array($cfgHost)) {
			$hosts = array_keys($cfgHost);
			$priorities = array_values($cfgHost);
		}
		$serversCount = count($hosts);
		if (is_int($cfgPort) || is_string($cfgPort)) {
			$ports = array_fill(0, $serversCount, intval($cfgPort));
		} else if (is_array($cfgPort)) {
			$ports = array_map('intval', array_values($cfgPort));
			if (count($ports) !== $serversCount)
				$ports = array_fill(0, $serversCount, $ports[0]);
		}
		foreach ($hosts as $index => $host)
			$this->memcached->addServer(
				$host, $ports[$index], $priorities[$index]
			);
	}

	/**
	 * Process every request connection or first persistent connection.
	 * @return bool
	 */
	protected function connectExecute () {
		$toolClass	= $this->application->GetToolClass();
		$version = $toolClass::Invoke(
			[$this->memcached, 'getVersion'], [],
			function ($errMsg, $errLevel, $errLine, $errContext) use (& $stats) {
				$version = NULL;
			}
		);
		return is_string($version);
	}

	/**
	 * Handle exception localy.
	 * @thrown \Exception|\Throwable
	 * @param  \Exception|\Throwable $e
	 * @return void
	 */
	protected function exceptionHandler ($e) {
		if ($this->application->GetEnvironment()->IsDevelopment()) {
			throw $e;
		} else {
			$debugClass = $this->application->GetDebugClass();
			$debugClass::Log($e);
		}
	}

	/**
	 * @inheritDoc
	 * @return \Memcached|NULL
	 */
	public function GetResource () {
		return $this->memcached;
	}

	/**
	 * @inheritDoc
	 * @param  \Memcached $resource
	 * @return \MvcCore\Ext\Caches\Memcached
	 */
	public function SetResource ($resource) {
		$this->memcached = $resource;
		return $this;
	}

	/**
	 * @inheritDoc
	 * @return \stdClass
	 */
	public function GetConfig () {
		return $this->config;
	}

	/**
	 * @inheritDoc
	 * @param  bool $enable
	 * @return \MvcCore\Ext\Caches\Memcached
	 */
	public function SetEnabled ($enabled) {
		if ($enabled) {
			$enabled = ($this->memcachedExists && (
				$this->connected === NULL ||
				$this->connected === TRUE
			));
		}
		$this->enabled = $enabled;
		return $this;
	}

	/**
	 * @inheritDoc
	 * @return bool
	 */
	public function GetEnabled () {
		return $this->enabled;
	}

	/**
	 * @inheritDoc
	 * @param  string   $key
	 * @param  mixed    $content
	 * @param  int|NULL $expirationSeconds
	 * @param  array    $cacheTags
	 * @return bool
	 */
	public function Save ($key, $content, $expirationSeconds = NULL, $cacheTags = []) {
		$result = FALSE;
		if (!$this->enabled)
			return $result;
		try {
			if ($expirationSeconds === NULL) {
				$this->memcached->set($key, $content);
			} else {
				$this->memcached->set($key, $content, time() + $expirationSeconds);
			}
			$this->setCacheTags([$key], $cacheTags);
			$result = TRUE;
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  array    $keysAndContents
	 * @param  int|NULL $expirationSeconds
	 * @param  array    $cacheTags
	 * @return bool
	 */
	public function SaveMultiple ($keysAndContents, $expirationSeconds = NULL, $cacheTags = []) {
		$result = FALSE;
		if (!$this->enabled || $keysAndContents === NULL)
			return $result;
		try {
			if ($expirationSeconds === NULL) {
				$this->memcached->setMulti($keysAndContents);
			} else {
				$this->memcached->setMulti($keysAndContents, time() + $expirationSeconds);
			}
			$this->setCacheTags(array_keys($keysAndContents), $cacheTags);
			$result = TRUE;
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  string        $key
	 * @param  callable|NULL $notFoundCallback function ($cache, $cacheKey) { ... $cache->Save($cacheKey, $data); return $data; }
	 * @return mixed|NULL
	 */
	public function Load ($key, callable $notFoundCallback = NULL) {
		$result = NULL;
		if (!$this->enabled) {
			if ($notFoundCallback !== NULL) {
				try {
					$result = call_user_func_array($notFoundCallback, [$this, $key]);
				} catch (\Exception $e1) { // backward compatibility
					$result = NULL;
					$this->exceptionHandler($e1);
				} catch (\Throwable $e2) {
					$result = NULL;
					$this->exceptionHandler($e2);
				}
			}
			return $result;
		}
		try {
			$rawResult = $this->memcached->get($key);
			if ($this->memcached->getResultCode() === \Memcached::RES_SUCCESS) {
				$result = $rawResult;
			} else if ($notFoundCallback !== NULL) {
				$result = call_user_func_array($notFoundCallback, [$this, $key]);
			}
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  \string[]     $keys
	 * @param  callable|NULL $notFoundCallback function ($cache, $cacheKey) { ... $cache->Save($cacheKey, $data); return $data; }
	 * @return array|NULL
	 */
	public function LoadMultiple (array $keys, callable $notFoundCallback = NULL) {
		$results = [];
		if (!$this->enabled) {
			if ($notFoundCallback !== NULL) {
				foreach ($keys as $index => $key) {
					try {
						$results[$index] = call_user_func_array(
							$notFoundCallback, [$this, $key]
						);
					} catch (\Exception $e1) { // backward compatibility
						$results[$index] = NULL;
						$this->exceptionHandler($e1);
					} catch (\Throwable $e2) {
						$results[$index] = NULL;
						$this->exceptionHandler($e2);
					}
				}
				return $results;
			} else {
				return NULL;
			}
		}
		try {
			$rawContents = $this->memcached->getMulti($keys);
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		foreach ($keys as $index => $key) {
			try {
				if (array_key_exists($key, $rawContents)) {
					$results[$index] = $rawContents[$key];
				} else if ($notFoundCallback !== NULL) {
					$results[$index] = call_user_func_array($notFoundCallback, [$this, $key]);
				}
			} catch (\Exception $e1) { // backward compatibility
				$results[$index] = NULL;
				$this->exceptionHandler($e1);
			} catch (\Throwable $e2) {
				$results[$index] = NULL;
				$this->exceptionHandler($e2);
			}
		}
		return $results;
	}

	/**
	 * @inheritDoc
	 * @param  string $key
	 * @return bool
	 */
	public function Delete ($key) {
		if (!$this->enabled) return FALSE;
		$deleted = FALSE;
		try {
			$deleted = $this->memcached->delete($key);
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $deleted;
	}

	/**
	 * DeleteMultiple(['usernames_active_ext'], ['usernames_active_ext' => ['user', 'externals']]);
	 * @inheritDoc
	 * @param  \string[] $keys
	 * @param  array     $keysTags
	 * @return int
	 */
	public function DeleteMultiple (array $keys, array $keysTags = []) {
		if (!$this->enabled) return 0;
		$deletedKeysCount = 0;
		try {
			if (count($keys) > 0) {
				$deletedKeysCount = call_user_func_array(
					[$this->memcached, 'deleteMulti'],
					[$keys]
				);
			}
			if (count($keysTags) > 0) {
				$change = FALSE;
				$newTags = [];
				$tags2Remove = [];
				foreach ($keysTags as $cacheKey => $cacheTags) {
					foreach ($cacheTags as $cacheTag) {
						$cacheTagFullKey = self::TAG_PREFIX . $cacheTag;
						$cacheTagKeysSet = $this->memcached->get($cacheTagFullKey);
						if ($cacheTagKeysSet !== FALSE) {
							$tagIndex = array_search($cacheKey, $cacheTagKeysSet, TRUE);
							if ($tagIndex !== FALSE) {
								array_splice($cacheTagKeysSet, $tagIndex, 1);
								$change = TRUE;
							}
						}
						if (count($cacheTagKeysSet) > 0) {
							$newTags[$cacheTagFullKey] = $cacheTagKeysSet;
						} else {
							$tags2Remove[] = $cacheTagFullKey;
						}
					}
				}
				if ($change)
					$this->memcached->setMulti($newTags);
				if (count($tags2Remove) > 0)
					$this->memcached->deleteMulti($tags2Remove);
			}
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $deletedKeysCount;
	}

	/**
	 * @inheritDoc
	 * @param  string|array $tags
	 * @return int
	 */
	public function DeleteByTags ($tags) {
		if (!$this->enabled) return 0;
		$tagsArr = func_get_args();
		if (count($tagsArr) === 1) {
			if (is_array($tags)) {
				$tagsArr = $tags;
			} else if (is_string($tags)) {
				$tagsArr = [$tags];
			}
		}
		$keysToDelete = [];
		foreach ($tagsArr as $tag) {
			$cacheTag = self::TAG_PREFIX . $tag;
			$keysToDelete[$cacheTag] = TRUE;
			$keys2DeleteLocal = $this->memcached->get($cacheTag);
			if ($keys2DeleteLocal !== FALSE)
				foreach ($keys2DeleteLocal as $key2DeleteLocal)
					$keysToDelete[$key2DeleteLocal] = TRUE;
		}
		$deletedKeysCount = 0;
		if (count($keysToDelete) > 0) {
			try {
				$deletedKeysCount = call_user_func_array(
					[$this->memcached, 'deleteMulti'],
					[array_keys($keysToDelete)]
				);
			} catch (\Exception $e1) { // backward compatibility
				$this->exceptionHandler($e1);
			} catch (\Throwable $e2) {
				$this->exceptionHandler($e2);
			}
		}
		return $deletedKeysCount;
	}

	/**
	 * @inheritDoc
	 * @param  string $key
	 * @return bool
	 */
	public function Has ($key) {
		$result = FALSE;
		if (!$this->enabled) return $result;
		try {
			$this->memcached->get($key);
			$result = $this->memcached->getResultCode() === \Memcached::RES_SUCCESS;
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  string|\string[] $keys
	 * @return int
	 */
	public function HasMultiple ($keys) {
		$result = 0;
		if (!$this->enabled) return $result;
		$keysArr = func_get_args();
		if (count($keysArr) === 1) {
			if (is_array($keys)) {
				$keysArr = $keys;
			} else if (is_string($keys)) {
				$keysArr = [$keys];
			}
		}
		try {
			$allResults = call_user_func_array(
				[$this->memcached, 'getMulti'],
				$keysArr
			);
			if ($allResults !== FALSE)
				$result = count($allResults);
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @return bool
	 */
	public function Clear () {
		$result = FALSE;
		if (!$this->enabled) return $result;
		try {
			$result = $this->memcached->flush();
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}
	
	/**
	 * Set up cache tag records if necessary:
	 * @param  \string[] $cacheKeys 
	 * @param  \string[] $cacheTags 
	 * @return \MvcCore\Ext\Caches\Memcached
	 */
	protected function setCacheTags ($cacheKeys = [], $cacheTags = []) {
		if (count($cacheTags) === 0)
			return $this;
		$change = FALSE;
		$newTags = [];
		foreach ($cacheKeys as $cacheKey) {
			foreach ($cacheTags as $cacheTag) {
				$cacheTagFullKey = self::TAG_PREFIX . $cacheTag;
				$cacheTagKeysSet = $this->memcached->get($cacheTagFullKey);
				if ($cacheTagKeysSet === FALSE) {
					$cacheTagKeysSet = [$cacheKey];
					$change = TRUE;
				} else if (array_search($cacheKey, $cacheTagKeysSet, TRUE) === FALSE) {
					$cacheTagKeysSet[] = $cacheKey;
					$change = TRUE;
				}
				$newTags[$cacheTagFullKey] = $cacheTagKeysSet;
			}
		}
		if ($change)
			$this->memcached->setMulti($newTags);
		return $this;
	}

}