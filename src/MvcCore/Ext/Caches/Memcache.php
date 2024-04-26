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

/**
 * @method static \MvcCore\Ext\Caches\Memcache GetInstance(string|array|NULL $connectionArguments,...)
 * Create or get cached cache wrapper instance.
 * If first argument is string, it's used as connection name.
 * If first argument is array, it's used as connection config array with keys:
 *  - `name`     default: `default`,
 *  - `host`     default: `127.0.0.1`, it could be single IP string 
 *               or an array of IPs and weights for multiple servers:
 *               `['192.168.0.10' => 1, '192.168.0.11' => 2, ...]`,
 *  - `port`     default: 11211, it could be single port integer 
 *               for single or multiple servers or an array 
 *               of ports for multiple servers: `[11211, 11212, ...]`,
 *  - `database` default: `$_SERVER['SERVER_NAME']`,
 *  - `timeout`  default: `1`, in seconds,
 *  - `provider` default: `FALSE`, boolean to compress data.
 *  If no argument provided, there is returned `default` 
 *  connection name with default initial configuration values.
 * @method \Memcache|NULL GetProvider() Get `\Memcache` provider instance.
 * @method \MvcCore\Ext\Caches\Memcache SetProvider(\Memcache|NULL $provider) Set `\Memcache` provider instance.
 * @property Memcache|NULL $provider
 */
class		Memcache
extends		\MvcCore\Ext\Caches\Base
implements	\MvcCore\Ext\ICache {
	
	/**
	 * MvcCore Extension - Cache - Memcache (older) - version:
	 * Comparison by PHP function version_compare();
	 * @see http://php.net/manual/en/function.version-compare.php
	 */
	const VERSION = '5.2.2';

	/**
	 * Provider configuration keys.
	 */
	const 
		PROVIDER_COMPRESS		= 'compress',
		PROVIDER_THRESHOLD		= 'threshold',
		PROVIDER_MIN_SAVINGS	= 'min_savings';

	/** @var array */
	protected static $defaults	= [
		\MvcCore\Ext\ICache::CONNECTION_PERSISTENCE	=> 'default',
		\MvcCore\Ext\ICache::CONNECTION_NAME		=> NULL,
		\MvcCore\Ext\ICache::CONNECTION_HOST		=> '127.0.0.1',
		\MvcCore\Ext\ICache::CONNECTION_PORT		=> 11211,
		\MvcCore\Ext\ICache::CONNECTION_TIMEOUT		=> 0.5, // in seconds
		\MvcCore\Ext\ICache::PROVIDER_CONFIG		=> [
			/** @see https://www.php.net/manual/en/memcache.setcompressthreshold */
			self::PROVIDER_COMPRESS					=> FALSE,	// `TRUE` $toolClass compress $cachedFiles records,
			self::PROVIDER_THRESHOLD				=> 32768,	// Do not compress cache records under this length limit,
			self::PROVIDER_MIN_SAVINGS				=> 0.2,		// `20%` amount of savings.
		],
	];

	/**
	 * `TRUE` if `igbinary` PHP extension installed.
	 * @var bool
	 */
	protected $igbinary = FALSE;

	/**
	 * Database prefix for each memcache key.
	 * @var string
	 */
	protected $prefix = '';

	/**
	 * `Memcache` write flags to compress data with zlib.
	 * @var int
	 */
	protected $writeFlags = 0;

	/**
	 * @inheritDoc
	 * @param array $config Connection config array with keys:
	 *  - `name`     default: `default`,
	 *  - `host`     default: `127.0.0.1`, it could be single IP string 
	 *               or an array of IPs and weights for multiple servers:
	 *               `['192.168.0.10' => 1, '192.168.0.11' => 2, ...]`,
	 *  - `port`     default: 11211, it could be single port integer 
	 *               for single or multiple servers or an array 
	 *               of ports for multiple servers: `[11211, 11212, ...]`,
	 *  - `database` default: `$_SERVER['SERVER_NAME']`,
	 *  - `timeout`  default: `1`, in seconds,
	 *  - `provider` default: `FALSE`, boolean to compress data.
	 */
	protected function __construct (array $config = []) {
		parent::__construct($config);
		$this->installed = class_exists('\Memcache');
		$this->igbinary = function_exists('igbinary_serialize');
		$dbKey		= self::CONNECTION_DATABASE;
		if (isset($this->config->{$dbKey}))
			$this->prefix = $this->config->{$dbKey} . ':';
	}

	/**
	 * @inheritDoc
	 * @return bool
	 */
	public function Connect () {
		if ($this->connected) {
			return TRUE;
		} else if (!$this->installed) {
			$this->enabled = FALSE;
			$this->connected = FALSE;
		} else {
			try {
				set_error_handler(function ($level, $msg, $file = NULL, $line = NULL, $err = NULL) {
					throw new \Exception($msg, $level);
				}, E_NOTICE);
				$this->provider = new \Memcache();
				$this->connectConfigure();
				$this->enabled = (
					$this->connected = $this->provider->getVersion() !== FALSE // real server or server pool connection execution
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
			restore_error_handler();
		}
		return $this->connected;
	}
	
	/**
	 * Configure connection provider before connection is established.
	 * @return void
	 */
	protected function connectConfigure () {
		// configure connection pool:
		$hosts = [];
		$ports = [];
		$weights = [];
		$cfgHost = $this->config->{self::CONNECTION_HOST};
		$cfgPort = $this->config->{self::CONNECTION_PORT};
		$persistent = isset($this->config->{self::CONNECTION_PERSISTENCE});
		$timeoutKey = self::CONNECTION_TIMEOUT;
		$timeout = isset($this->config->{$timeoutKey})
			? $this->config->{$timeoutKey}
			: static::$defaults[$timeoutKey];
		if (is_string($cfgHost)) {
			$hosts = [$cfgHost];
			$weights = [1];
		} else if (is_array($cfgHost)) {
			$hosts = array_keys($cfgHost);
			$weights = array_values($cfgHost);
		}
		$serversCount = count($hosts);
		if (is_int($cfgPort) || is_string($cfgPort)) {
			$ports = array_fill(0, $serversCount, intval($cfgPort));
		} else if (is_array($cfgPort)) {
			$ports = array_map('intval', array_values($cfgPort));
			if (count($ports) !== $serversCount)
				$ports = array_fill(0, $serversCount, $ports[0]);
		}
		foreach ($hosts as $index => $host) {
			$this->provider->addServer(
				$host, $ports[$index], $persistent, $weights[$index], $timeout
			);
			$this->provider->getServerStatus($host, $ports[$index]);
		}
		// configure compression if necessary:
		$provKey = self::PROVIDER_CONFIG;
		$providerConfig = isset($this->config->{$provKey})
			? $this->config->{$provKey}
			: [];
		$config = array_merge([], static::$defaults[$provKey], $providerConfig);
		if ($config[self::PROVIDER_COMPRESS]) {
			$this->writeFlags = constant('MEMCACHE_COMPRESSED');
			$thresold = $config[self::PROVIDER_THRESHOLD];
			$minSavings = $config[self::PROVIDER_MIN_SAVINGS];
			$this->provider->setCompressThreshold($thresold, $minSavings);
		}
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
			if ($this->igbinary)
				$content = igbinary_serialize($content);
			if ($expirationSeconds === NULL) {
				$this->provider->set($this->prefix . $key, $content);
			} else {
				$this->provider->set($this->prefix . $key, $content, $this->writeFlags, time() + $expirationSeconds);
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
			$count = 0;
			if ($expirationSeconds === NULL) {
				foreach ($keysAndContents as $key => $content)
					if ($this->provider->set($this->prefix . $key, $content))
						$count++;
			} else {
				$ttl = time() + $expirationSeconds;
				foreach ($keysAndContents as $key => $content)
					if ($this->provider->set($this->prefix . $key, $content, $this->writeFlags, $ttl))
						$count++;
			}
			$this->setCacheTags(array_keys($keysAndContents), $cacheTags);
			$result = count($keysAndContents) === $count;
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
			$flags = FALSE;
			$rawResult = $this->provider->get($this->prefix . $key, $flags);
			if ($flags !== FALSE) {
				$result = $this->igbinary
					? igbinary_unserialize($rawResult)
					: $rawResult;
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
		foreach ($keys as $index => $key) {
			try {
				$flags = FALSE;
				$rawResult = $this->provider->get($this->prefix . $key, $flags);
				if ($flags !== FALSE) {
					$results[$index] = $this->igbinary
						? igbinary_unserialize($rawResult)
						: $rawResult;
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
			$deleted = $this->provider->delete($this->prefix . $key);
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
				foreach ($keys as $key) {
					if ($this->provider->delete($this->prefix . $key))
						$deletedKeysCount++;
				}
			}
			if (count($keysTags) > 0) {
				$change = FALSE;
				$newTags = [];
				$tags2Remove = [];
				foreach ($keysTags as $cacheKey => $cacheTags) {
					foreach ($cacheTags as $cacheTag) {
						$cacheTagFullKey = $this->prefix . self::TAG_PREFIX . $cacheTag;
						$flags = FALSE;
						$cacheTagKeysSet = $this->provider->get($cacheTagFullKey, $flags);
						if ($flags !== FALSE) {
							if ($this->igbinary)
								$cacheTagKeysSet = igbinary_unserialize($cacheTagKeysSet);
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
				if ($change) {
					foreach ($newTags as $cacheTagFullKey => $cacheTagKeysSet)
						$this->provider->set($cacheTagFullKey, $cacheTagKeysSet);
				}
				if (count($tags2Remove) > 0) {
					foreach ($tags2Remove as $cacheTagFullKey)
						$this->provider->delete($cacheTagFullKey);
				}
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
			$cacheTag = $this->prefix . self::TAG_PREFIX . $tag;
			$keysToDelete[$cacheTag] = TRUE;
			$flags = FALSE;
			$keys2DeleteLocal = $this->provider->get($cacheTag);
			if ($flags !== FALSE) {
				if ($this->igbinary)
					$keys2DeleteLocal = igbinary_unserialize($keys2DeleteLocal);
				foreach ($keys2DeleteLocal as $key2DeleteLocal)
					$keysToDelete[$key2DeleteLocal] = TRUE;
			}
		}
		$deletedKeysCount = 0;
		if (count($keysToDelete) > 0) {
			try {
				foreach (array_keys($keysToDelete) as $keyToDelete)
					if ($this->provider->delete($keyToDelete))
						$deletedKeysCount++;
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
			$flags = FALSE;
			$this->provider->get($this->prefix . $key, $flags);
			$result = $flags !== FALSE;
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
			foreach ($keysArr as $key) {
				$flags = FALSE;
				$this->provider->get($this->prefix . $key, $flags);
				if ($flags !== FALSE) $result++;
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
	 * @return bool
	 */
	public function Clear () {
		$result = FALSE;
		if (!$this->enabled) return $result;
		try {
			$result = $this->provider->flush();
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
	 * @return \MvcCore\Ext\Caches\Memcache
	 */
	protected function setCacheTags ($cacheKeys = [], $cacheTags = []) {
		if (count($cacheTags) === 0)
			return $this;
		$change = FALSE;
		$newTags = [];
		foreach ($cacheKeys as $cacheKey) {
			foreach ($cacheTags as $cacheTag) {
				$cacheTagFullKey = $this->prefix . self::TAG_PREFIX . $cacheTag;
				$flags = FALSE;
				$cacheTagKeysSet = $this->provider->get($cacheTagFullKey, $flags);
				if ($flags === FALSE) {
					$cacheTagKeysSet = [$cacheKey];
					$change = TRUE;
				} else {
					if ($this->igbinary)
						$cacheTagKeysSet = igbinary_unserialize($cacheTagKeysSet);
					if (array_search($cacheKey, $cacheTagKeysSet, TRUE) === FALSE) {
						$cacheTagKeysSet[] = $cacheKey;
						$change = TRUE;
					}
				}
				$newTags[$cacheTagFullKey] = $cacheTagKeysSet;
			}
		}
		if ($change) {
			foreach ($newTags as $cacheTagFullKey => $cacheTagKeysSet) {
				if ($this->igbinary)
					$cacheTagKeysSet = igbinary_serialize($cacheTagKeysSet);
				$this->provider->set($cacheTagFullKey, $cacheTagKeysSet);
			}
		}
		return $this;
	}

}