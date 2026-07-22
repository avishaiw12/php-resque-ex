<?php
require_once dirname(__FILE__) . '/Resque/Event.php';
require_once dirname(__FILE__) . '/Resque/Exception.php';

/**
 * Base Resque class.
 *
 * @package		Resque
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class Resque
{
    const VERSION = '1.2.5';
    const WORKERS = 'workers';
    const CURRENT_JOBS = 'current_jobs';
    const WORKER_LOGGER = 'workerLogger';
    const WORKER_PREFIX = 'worker:';
    const PROCESSED = 'processed';
    const PROCESSED_PREFIX = self::PROCESSED . ':';
    const FAILED = 'failed';
    const FAILED_PREFIX = self::FAILED . ':';
    const STARTED_SUFFIX = ':started';
    const PING_SUFFIX = ':ping';
    const SCHEDULER_IDENTIFIER = '-scheduler-';

    /**
     * @var Resque_Redis Instance of Resque_Redis that talks to redis.
     */
    public static $redis = null;

    /**
     * @var mixed Host/port conbination separated by a colon, or a nested
     * array of server swith host/port pairs
     */
    protected static $redisServer = null;

    /**
     * @var int ID of Redis database to select.
     */
    protected static $redisDatabase = 0;

    /**
     * @var string namespace of the redis keys
     */
    protected static $namespace = '';

    /**
     * @var string password for the redis server
     */
    protected static $password = null;

    /**
     * @var int PID of current process. Used to detect changes when forking
     *  and implement "thread" safety to avoid race conditions.
     */
    protected static $pid = null;

    /**
     * Given a host/port combination separated by a colon, set it as
     * the redis server that Resque will talk to.
     *
     * @param mixed $server Host/port combination separated by a colon, or
     *                      a nested array of servers with host/port pairs.
     * @param int $database
     */
    public static function setBackend($server, $database = 0, $namespace = 'resque', $password = null)
    {
        self::$redisServer = $server;
        self::$redisDatabase = $database;
        self::$redis = null;
        self::$namespace = $namespace;
        self::$password = $password;
    }

    /**
     * Return an instance of the Resque_Redis class instantiated for Resque.
     *
     * @return Resque_Redis Instance of Resque_Redis.
     */
    public static function redis()
    {
        // Detect when the PID of the current process has changed (from a fork, etc)
        // and force a reconnect to redis.
        $pid = getmypid();
        if (self::$pid !== $pid) {
            self::$redis = null;
            self::$pid = $pid;
        }

        if (!is_null(self::$redis)) {
            return self::$redis;
        }

        $server = self::$redisServer;
        if (empty($server)) {
            $server = 'localhost:6379';
        }

        if (is_array($server)) {
            require_once dirname(__FILE__) . '/Resque/RedisCluster.php';
            self::$redis = new Resque_RedisCluster($server);
        } else {
            if (strpos($server, 'unix:') === false) {
                list($host, $port) = explode(':', $server);
            } else {
                $host = $server;
                $port = null;
            }
            require_once dirname(__FILE__) . '/Resque/Redis.php';
            $redisInstance = new Resque_Redis($host, $port, self::$password);
            $redisInstance->prefix(self::$namespace);
            self::$redis = $redisInstance;
        }

        if (!empty(self::$redisDatabase)) {
            self::$redis->select(self::$redisDatabase);
        }

        return self::$redis;
    }

    /**
     * Push a job to the end of a specific queue. If the queue does not
     * exist, then create it as well.
     *
     * @param string $queue The name of the queue to add the job to.
     * @param array $item Job description as an array to be JSON encoded.
     */
    public static function push($queue, $item)
    {
        self::redis()->sadd('queues', $queue);
        self::redis()->rpush('queue:' . $queue, json_encode($item));
    }

    /**
     * Pop an item off the end of the specified queue, decode it and
     * return it.
     *
     * @param string $queue The name of the queue to fetch an item from.
     * @return array Decoded item from the queue.
     */
    public static function pop($queue)
    {
        $item = self::redis()->lpop('queue:' . $queue);
        if (!$item) {
            return;
        }

        return json_decode($item, true);
    }

    /**
     * Remove items of the specified queue
     *
     * @param string $queue The name of the queue to fetch an item from.
     * @param array $items
     * @return integer number of deleted items
     */
    public static function dequeue($queue, $items = [])
    {
        if (count($items) > 0) {
            return self::removeItems($queue, $items);
        } else {
            return self::removeList($queue);
        }
    }

    /**
     * Return the size (number of pending jobs) of the specified queue.
     *
     * @param $queue name of the queue to be checked for pending jobs
     *
     * @return int The size of the queue.
     */
    public static function size($queue)
    {
        return self::redis()->llen('queue:' . $queue);
    }

    /**
     * Create a new job and save it to the specified queue.
     *
     * @param string $queue The name of the queue to place the job in.
     * @param string $class The name of the class that contains the code to execute the job.
     * @param array $args Any optional arguments that should be passed when the job is executed.
     * @param boolean $trackStatus Set to true to be able to monitor the status of a job.
     *
     * @return string
     */
    public static function enqueue($queue, $class, $args = null, $trackStatus = false)
    {
        require_once dirname(__FILE__) . '/Resque/Job.php';
        $result = Resque_Job::create($queue, $class, $args, $trackStatus);
        if ($result) {
            Resque_Event::trigger('afterEnqueue', [
                'class' => $class,
                'args'  => $args,
            ]);
        }

        return $result;
    }

    /**
     * Reserve and return the next available job in the specified queue.
     *
     * @param string $queue Queue to fetch next available job from.
     * @return Resque_Job Instance of Resque_Job to be processed, false if none or error.
     */
    public static function reserve($queue)
    {
        require_once dirname(__FILE__) . '/Resque/Job.php';

        return Resque_Job::reserve($queue);
    }

    /**
     * Get an array of all known queues.
     *
     * @return array Array of queues.
     */
    public static function queues()
    {
        $queues = self::redis()->smembers('queues');
        if (!is_array($queues)) {
            $queues = [];
        }

        return $queues;
    }

    /**
     * Remove Items from the queue
     * Safely moving each item to a temporary queue before processing it
     * If the Job matches, counts otherwise puts it in a requeue_queue
     * which at the end eventually be copied back into the original queue
     *
     * @private
     *
     * @param string $queue The name of the queue
     * @param array $items
     * @return integer number of deleted items
     */
    private static function removeItems($queue, $items = [])
    {
        $counter = 0;
        $originalQueue = 'queue:' . $queue;
        $tempQueue = $originalQueue . ':temp:' . time();
        $requeueQueue = $tempQueue . ':requeue';

        // move each item from original queue to temp queue and process it
        $finished = false;
        while (!$finished) {
            $string = self::redis()->rpoplpush($originalQueue, self::redis()->getPrefix() . $tempQueue);

            if (!empty($string)) {
                if (self::matchItem($string, $items)) {
                    self::redis()->rpop($tempQueue);
                    $counter++;
                } else {
                    self::redis()->rpoplpush($tempQueue, self::redis()->getPrefix() . $requeueQueue);
                }
            } else {
                $finished = true;
            }
        }

        // move back from temp queue to original queue
        $finished = false;
        while (!$finished) {
            $string = self::redis()->rpoplpush($requeueQueue, self::redis()->getPrefix() . $originalQueue);
            if (empty($string)) {
                $finished = true;
            }
        }

        // remove temp queue and requeue queue
        self::redis()->del($requeueQueue);
        self::redis()->del($tempQueue);

        return $counter;
    }

    /**
     * matching item
     * item can be ['class'] or ['class' => 'id'] or ['class' => {:foo => 1, :bar => 2}]
     * @private
     *
     * @params string $string redis result in json
     * @params $items
     *
     * @return (bool)
     */
    private static function matchItem($string, $items)
    {
        $decoded = json_decode($string, true);

        foreach ($items as $key => $val) {
            # class name only  ex: item[0] = ['class']
            if (is_numeric($key)) {
                if ($decoded['class'] == $val) {
                    return true;
                }
                # class name with args , example: item[0] = ['class' => {'foo' => 1, 'bar' => 2}]
            } elseif (is_array($val)) {
                $decodedArgs = (array)$decoded['args'][0];
                if ($decoded['class'] == $key &&
                    count($decodedArgs) > 0 && count(array_diff($decodedArgs, $val)) == 0
                ) {
                    return true;
                }
                # class name with ID, example: item[0] = ['class' => 'id']
            } else {
                if ($decoded['class'] == $key && $decoded['id'] == $val) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Remove List
     *
     * @private
     *
     * @params string $queue the name of the queue
     * @return integer number of deleted items belongs to this list
     */
    private static function removeList($queue)
    {
        $counter = self::size($queue);
        $result = self::redis()->del('queue:' . $queue);

        return ($result == 1) ? $counter : 0;
    }

    /*
     * Generate an identifier to attach to a job for status tracking.
     *
     * @return string
     */
    public static function generateJobId()
    {
        return md5(uniqid('', true));
    }

    public static function getInProgressJobsCount(?string $workersPrefix = null): int {
        if (empty($workersPrefix)) {
            return self::redis()->hlen(self::CURRENT_JOBS);
        }

        $keys = self::redis()->hKeys(self::CURRENT_JOBS);
        return count(array_filter($keys, function ($key) use($workersPrefix) {
            return self::isEnvWorker($key, $workersPrefix);
        }));
    }

    /**
     * Clean workers that were terminated after timeout
     *
     * @return array of unfinished jobs
     */
    public static function cleanWorkers(?string $workersPrefix = null): array {
        $notFinishedJobs = [];
        $workers = self::redis()->sMembers(self::WORKERS);
        foreach ($workers as $workerId) {
            if (self::isEnvWorker($workerId, $workersPrefix) && !self::isWorkerAliveByPing($workerId)) {
                $notFinishedJob = self::redis()->hget(self::CURRENT_JOBS, $workerId);
                if ($notFinishedJob) {
                    $notFinishedJobs[] = $notFinishedJob;
                }
                self::workerCleanup($workerId);
            }
        }

        return $notFinishedJobs;
    }

    public static function getJobsToRerun(?string $workersPrefix = null, int $jobTimeout = 60, array $workersToRerun = []) {
        $jobsToRerun = [];
        $workers = self::redis()->hKeys(self::CURRENT_JOBS);
        $now = date_timestamp_get(date_create());
        foreach ($workers as $workerId) {
            if (self::isEnvWorker($workerId, $workersPrefix)) {
                $notFinishedJob = self::redis()->hget(self::CURRENT_JOBS, $workerId);
                if ($notFinishedJob) {
                    $notFinishedJob = json_decode($notFinishedJob, true);
                    $runAt = date_timestamp_get(date_create_from_format('D M d H:i:s e Y', $notFinishedJob['run_at']));
                    if (in_array($notFinishedJob['payload']['class'], $workersToRerun) && ($now - $runAt) > $jobTimeout
                        && !array_key_exists("rerun", $notFinishedJob['payload']['args'][0])) {
                        $jobsToRerun[] = $notFinishedJob;
                        self::redis()->hdel(self::CURRENT_JOBS, $workerId);
                    }
                }
            }
        }
        return $jobsToRerun;
    }

    public static function workerCleanup(string $workerId) {
        try {
            self::redis()->multi();
            self::redis()->srem(self::WORKERS, $workerId);
            self::redis()->hdel(self::CURRENT_JOBS, $workerId);
            self::redis()->del(self::WORKER_PREFIX . $workerId . self:: STARTED_SUFFIX);
            Resque_Stat::clear(self::PROCESSED_PREFIX . $workerId);
            Resque_Stat::clear(self::FAILED_PREFIX . $workerId);
            self::redis()->hdel(self::WORKER_LOGGER, $workerId);
            self::redis()->del(self::WORKER_PREFIX . $workerId . self::PING_SUFFIX);
            self::redis()->exec();
        } catch (Throwable $t) {
            error_log("Worker cleanup error: " .  $workerId);
        }
    }

    private static function isWorkerAliveByPing(string $workerId): bool {
        return self::redis()->get(self::WORKER_PREFIX . $workerId . self::PING_SUFFIX) !== false;
    }

    private static function isEnvWorker(string $workerId, ?string $workersPrefix = null): bool {
       return  (empty($workersPrefix) || strpos($workerId, $workersPrefix) === 0)
           && strpos($workerId, self::SCHEDULER_IDENTIFIER) === false;
    }
}
