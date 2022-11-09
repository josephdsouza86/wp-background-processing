<?php
if ( !class_exists( 'AL_Background_Process' ) ) {
/**
 * WP Background Process
 *
 * @package WP-Background-Processing
 */

/**
 * Abstract AL_Background_Process class.
 *
 * @abstract
 * @extends AL_Async_Request
 */
    abstract class AL_Background_Process extends AL_Async_Request {

        /**
         * Action
         *
         * (default value: 'background_process')
         *
         * @var string
         * @access protected
         */
        protected $action = 'background_process';

        /**
         * Start time of current process.
         *
         * (default value: 0)
         *
         * @var int
         * @access protected
         */
        protected $start_time = 0;

        /**
         * Cron_hook_identifier
         *
         * @var mixed
         * @access protected
         */
        protected $cron_hook_identifier;

        /**
         * Cron_interval_identifier
         *
         * @var mixed
         * @access protected
         */
        protected $cron_interval_identifier;

        /**
         * Persist the unique key for this batch during
         * creeation, so that the queue can be saved multiple
         * times under the same key
         */
        protected $unique_batch_key;

        /**
         * Initiate new background process
         */
        public function __construct() {
            parent::__construct();

            $this->cron_hook_identifier = $this->identifier . '_cron';
            $this->cron_interval_identifier = $this->identifier . '_cron_interval';

            add_action( $this->cron_hook_identifier, array( $this, 'handle_cron_healthcheck' ) );
            add_filter( 'cron_schedules', array( $this, 'schedule_cron_healthcheck' ) );
        }

        /**
         * Dispatch
         *
         * @access public
         * @return void
         */
        public function dispatch( $scheduled_time = null ) {
            // Schedule the cron healthcheck.
            $this->schedule_event( $scheduled_time );

            if ( function_exists( 'custom_log_file' ) ) {
                $items_left = count( $this->data );
                custom_log_file( "[Dispatch] $items_left items in queue.", "$this->unique_batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
            }

            if ( is_null( $scheduled_time ) || $scheduled_time < time() ) {
                // Execute immediately, perform remote post.
                return parent::dispatch();
            } else {
                return $scheduled_time;
            }
        }

        /**
         * Push to queue
         *
         * @param mixed $data Data.
         *
         * @return $this
         */
        public function push_to_queue( $data, $key = null ) {            
            if ($key) {                
				if ( isset($this->data[$key]) ) {
                    if ( function_exists( 'custom_log_file' ) ) {
                        custom_log_file( "[Updated] $key item already in queue.", "$this->unique_batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                    }
                }
                $this->data[$key] = $data;        
            } else {
                $this->data[] = $data;        
            }

            return $this;
        }

        /**
         * Save queue
         *
         * @return $this
         */
        public function save() {
            if ( !$this->unique_batch_key || ( $this->unique_batch_key && $this->is_batch_running( $this->unique_batch_key ) ) ) {
                // Ensure we have a unique batch key. Don't reuse a batch key if it is being
                // processed (as adding and removing will clash)
                $this->unique_batch_key = $this->generate_key();
            }

            if ( !empty( $this->data ) ) {
                // $added = $this->save_batch( $this->unique_batch_key, $this->data );
                $added = update_site_option( $this->unique_batch_key, $this->data );
                if ( function_exists( 'custom_log_file' ) ) {
                    if ( !$added ) {
                        custom_log_file( "[Save failed] Couldn't add item to the queue.", "$this->unique_batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                    } else {
                        $items_left = count( $this->data );
                        custom_log_file( "[Saved] $items_left items in queue.", "$this->unique_batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                    }
                }
            }

            return $this;
        }


        /**
         * Saves items to the batch only if it is not processing, otherwise 
         * it starts a fresh batch
         */
        public function safe_push_to_queue_and_save ( $data, $key = null ) {
            if ( !$this->unique_batch_key || ( $this->unique_batch_key && $this->is_batch_running( $this->unique_batch_key ) ) ) {
                // Ensure we have a unique batch key. Don't reuse a batch key if it is being
                // processed (as adding and removing will clash)
                $this->unique_batch_key = $this->generate_key();

                // Reset data
                $this->delete( $this->unique_batch_key );
                $this->data = [];
            }

            return $this->push_to_queue( $data, $key )->save();
        }

        /**
         * Get queue status label
         * 
         * Indicates whether there is a scheduled task running a batch
         */
        public function get_queue_status () {
            if ($this->is_process_running()) {
                return "processing";
            } else if ( $this->is_queue_empty() ) {
                return "idle";
            }
            return "Unknown";
        }

        /**
         * Update queue
         *
         * @param string $key  Key.
         * @param array  $data Data.
         *
         * @return $this
         */
        public function update( $key, $data ) {
            if ( !empty( $data ) ) {
                update_site_option( $key, $data );
            }

            return $this;
        }

        /**
         * Delete queue
         *
         * @param string $key Key.
         *
         * @return $this
         */
        public function delete( $key ) {
            delete_site_option( $key );

            return $this;
        }

        /**
         * Generate key
         *
         * Generates a unique key based on microtime. Queue items are
         * given a unique key so that they can be merged upon save.
         *
         * @param int $length Length.
         *
         * @return string
         */
        protected function generate_key( $length = 64 ) {
            $unique = md5( microtime() . rand() );
            $prepend = $this->identifier . '_batch_';

            return substr( $prepend . $unique, 0, $length );
        }     

        /**
         * Maybe process queue
         *
         * Checks whether data exists within the queue and that
         * the process is not already running.
         */
        public function maybe_handle() {
            // Don't lock up other requests while processing
            session_write_close();

            if ( $this->is_process_running() ) {
                // Background process already running.
                wp_die();
            }

            if ( $this->is_queue_empty() ) {
                // No data to process.
                wp_die();
            }

            check_ajax_referer( $this->identifier, 'nonce' );

            $this->handle();

            wp_die();
        }

        /**
         * Is queue empty
         *
         * @return bool
         */
        protected function is_queue_empty() {
            global $wpdb;

            $table = $wpdb->options;
            $column = 'option_name';

            if ( is_multisite() ) {
                $table = $wpdb->sitemeta;
                $column = 'meta_key';
            }

            $key = $wpdb->esc_like( $this->identifier . '_batch_' ) . '%';

            $count = $wpdb->get_var( $wpdb->prepare( "
			SELECT COUNT(*)
			FROM {$table}
			WHERE {$column} LIKE %s
		", $key ) );

            return ( $count > 0 ) ? false : true;
        }

        protected function get_uncached_transient( $name ) {
            global $wpdb;
            
            $table = $wpdb->options;
            $column = 'option_name';

            if ( is_multisite() ) {
                $table = $wpdb->sitemeta;
                $column = 'meta_key';
            }
            
            $key = esc_sql( '_site_transient_' . $name );

            return $wpdb->get_var( $wpdb->prepare( "
			SELECT option_value
			FROM {$table}
			WHERE {$column} = %s
			LIMIT 1
		    ", $key ) );
        }

        /**
         * Is process running
         *
         * Check whether the current process is already running
         * in a background process.
         */
        protected function is_process_running() {
            if ( get_site_transient( $this->identifier . '_process_lock' ) ) {
                // Process already running.
                return true;
            }

            return false;
        }

        /**
         * Indicates if this actual batch is running
         */
        protected function is_batch_running( $batch_key ) {
            if ( !empty($batch_key) && $this->get_uncached_transient( $this->identifier . '_current_batch' ) == $batch_key ) {
                // Process already running.
                return true;
            }

            return false;
        }

        /**
         * Lock process
         *
         * Lock the process so that multiple instances can't run simultaneously.
         * Override if applicable, but the duration should be greater than that
         * defined in the time_exceeded() method.
         */
        protected function lock_process( $batch_key = null ) {
            $this->start_time = time(); // Set start time of current process.

            $lock_duration = ( property_exists( $this, 'queue_lock_time' ) ) ? $this->queue_lock_time : 60; // 1 minute
            $lock_duration = apply_filters( $this->identifier . '_queue_lock_time', $lock_duration );

            set_site_transient( $this->identifier . '_process_lock', microtime(), $lock_duration );

            if ( !empty( $batch_key ) ) {
                if ( function_exists( 'custom_log_file' ) ) {
                    custom_log_file( "[Info] Locking batch $batch_key", "$batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                }
                set_site_transient( $this->identifier . '_current_batch', $batch_key, $lock_duration );
            }
        }

        /**
         * Unlock process
         *
         * Unlock the process so that other instances can spawn.
         *
         * @return $this
         */
        protected function unlock_process() {
            delete_site_transient( $this->identifier . '_process_lock' );

            return $this;
        }

        /**
         * Get batch
         *
         * @return stdClass Return the first batch from the queue
         */
        protected function get_batch() {
            global $wpdb;

            $table = $wpdb->options;
            $column = 'option_name';
            $key_column = 'option_id';
            $value_column = 'option_value';

            if ( is_multisite() ) {
                $table = $wpdb->sitemeta;
                $column = 'meta_key';
                $key_column = 'meta_id';
                $value_column = 'meta_value';
            }

            $key = $wpdb->esc_like( $this->identifier . '_batch_' ) . '%';

            $query = $wpdb->get_row( $wpdb->prepare( "
                SELECT *
                FROM {$table}
                WHERE {$column} LIKE %s
                ORDER BY {$key_column} ASC
                LIMIT 1
            ", $key ) );

            $batch = new stdClass();
            $batch->key = $query->$column;
            $batch->data = maybe_unserialize( $query->$value_column );

            return $batch;
        }

        /**
         * Test method
         */
        protected function save_batch( $batch_key, $data ) {
            global $wpdb;

            $table = $wpdb->options;
            $name_column = 'option_name';
            $id_column = 'option_id';
            $value_column = 'option_value';

            if ( is_multisite() ) {
                $table = $wpdb->sitemeta;
                $name_column = 'meta_key';
                $id_column = 'meta_id';
                $value_column = 'meta_value';
            }

            // Does exist
            $exists = $wpdb->get_var( $wpdb->prepare( "
                SELECT option_id
                FROM {$table}
                WHERE {$name_column} = {$batch_key}
                LIMIT 1
            " ) );

            if ($exists) {
                // Update
                $result = $wpdb->update( $table, [ $value_column => maybe_serialize( $data ) ], [ $name_column => $batch_key ] );
            } else {
                // Insert
                $result = $wpdb->insert( $table, [ $name_column => $batch_key, $value_column => maybe_serialize( $data ), 'autoload' => 'no' ] );
            }

            return $result;
        }

        /**
         * Handle
         *
         * Pass each queue item to the task handler, while remaining
         * within server memory and time limit constraints.
         */
        protected function handle() {
            if ( !$this->is_queue_empty() && !$this->is_process_running() ) {
                // Get the batch to process
                $batch = $this->get_batch();
                $batch_key = $batch->key;
                $batch_size = count( $batch->data );

                // Indicate which batch is processing
                $this->lock_process( $batch_key );

                if ( function_exists( 'custom_log_file' ) ) {
                    custom_log_file( "[Info] Staring. $batch_size items in the batch ... ", "$batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                }

                try {
                    foreach ( $batch->data as $key => $value ) {
                        // Process this item
                        $task = $this->task( $value, $batch_key );

                        if ( false !== $task ) {
                            // Retain queue item for later
                            $batch->data[$key] = $task;
                        } else {
                            // Remove
                            unset( $batch->data[$key] );
                        }

                        // Check and update/delete current batch immediately after processing each item
                        if ( !empty( $batch->data ) ) {
                            $this->update( $batch_key, $batch->data );
                        } else {
                            $this->delete( $batch_key );
                            break;
                        }

                        if ( $this->time_exceeded() || $this->memory_exceeded() ) {
                            // Batch limits reached.
                            break;
                        }
                    }
                } catch ( Exception $ex ) {
                    /* Not sure what to do here at the moment */
                    if ( function_exists( 'custom_log_file' ) ) {
                        custom_log_file( "[Exception] {$ex->getMessage()}", "$batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                    }
                } finally {
                    // Update or delete current batch.
                    if ( !empty( $batch->data ) ) {
                        $this->update( $batch_key, $batch->data );
                    } else {
                        $this->delete( $batch_key );
                    }

                    if ( function_exists( 'custom_log_file' ) ) {
                        $items_left = count( $batch->data );
                        custom_log_file( "[Info] Batch stopped. $items_left items left.", "$batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                    }

                    $this->unlock_process();

                    // Start next batch or complete process.
                    if ( !$this->is_queue_empty() ) {
                        if ( function_exists( 'custom_log_file' ) ) {
                            custom_log_file( "[Info] Start next batch.", "$batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                        }
                        $this->dispatch();
                    } else {
                        if ( function_exists( 'custom_log_file' ) ) {
                            custom_log_file( "[Info] Complete.", "$batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                        }
                        $this->complete( $batch_key );
                    }
                }
            }

            wp_die();
        }

        /**
         * Memory exceeded
         *
         * Ensures the batch process never exceeds 90%
         * of the maximum WordPress memory.
         *
         * @return bool
         */
        protected function memory_exceeded() {
            $memory_limit = $this->get_memory_limit() * 0.9; // 90% of max memory
            $current_memory = memory_get_usage( true );
            $return = false;

            if ( $current_memory >= $memory_limit ) {
                $return = true;
            }

            return apply_filters( $this->identifier . '_memory_exceeded', $return );
        }

        /**
         * Get memory limit
         *
         * @return int
         */
        protected function get_memory_limit() {
            if ( function_exists( 'ini_get' ) ) {
                $memory_limit = ini_get( 'memory_limit' );
            } else {
                // Sensible default.
                $memory_limit = '128M';
            }

            if ( !$memory_limit || -1 === intval( $memory_limit ) ) {
                // Unlimited, set to 32GB.
                $memory_limit = '32000M';
            }

            return wp_convert_hr_to_bytes( $memory_limit );
        }

        /**
         * Time exceeded.
         *
         * Ensures the batch never exceeds a sensible time limit.
         * A timeout limit of 30s is common on shared hosting.
         *
         * @return bool
         */
        protected function time_exceeded() {
            $finish = $this->start_time + apply_filters( $this->identifier . '_default_time_limit', 20 ); // 20 seconds
            $return = false;

            if ( time() >= $finish ) {
                $return = true;
            }

            return apply_filters( $this->identifier . '_time_exceeded', $return );
        }

        /**
         * Complete.
         *
         * Override if applicable, but ensure that the below actions are
         * performed, or, call parent::complete().
         */
        protected function complete( $batch_key = null ) {
            // Unschedule the cron healthcheck.
            $this->clear_scheduled_event();
        }

        /**
         * Schedule cron healthcheck
         *
         * @access public
         *
         * @param mixed $schedules Schedules.
         *
         * @return mixed
         */
        public function schedule_cron_healthcheck( $schedules ) {
            $interval = apply_filters( $this->identifier . '_cron_interval', 5 );

            if ( property_exists( $this, 'cron_interval' ) ) {
                $interval = apply_filters( $this->identifier . '_cron_interval', $this->cron_interval );
            }

            // Adds every 5 minutes to the existing schedules.
            $schedules[$this->identifier . '_cron_interval'] = array(
                'interval' => MINUTE_IN_SECONDS * $interval,
                'display' => sprintf( __( 'Every %d Minutes' ), $interval ),
            );

            return $schedules;
        }

        /**
         * Handle cron healthcheck
         *
         * Restart the background process if not already running
         * and data exists in the queue.
         */
        public function handle_cron_healthcheck() {
            if ( $this->is_process_running() ) {
                // Background process already running.
                exit;
            }

            if ( $this->is_queue_empty() ) {
                // No data to process.
                $this->clear_scheduled_event();
                exit;
            }

            $this->handle();

            exit;
        }

        /**
         * Schedule event
         */
        protected function schedule_event( $scheduled_time = null ) {
            if ( !wp_next_scheduled( $this->cron_hook_identifier ) ) {
                wp_schedule_event( $scheduled_time ? $scheduled_time : time(), $this->cron_interval_identifier, $this->cron_hook_identifier );
                if ( function_exists( 'custom_log_file' ) ) {
                    $date = date( "d-m-Y H:i:s", $scheduled_time );
                    custom_log_file( "[Scheduled for $date]", "$this->unique_batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                }
            } else {
                if ( function_exists( 'custom_log_file' ) ) {
                    custom_log_file( "[Already Scheduled]", "$this->unique_batch_key.txt", "/al-log/$this->action/batch/" . date("Ymd") );
                }
            }
        }

        /**
         * Clear scheduled event
         */
        protected function clear_scheduled_event() {
            $timestamp = wp_next_scheduled( $this->cron_hook_identifier );

            if ( $timestamp ) {
                wp_unschedule_event( $timestamp, $this->cron_hook_identifier );
            }
        }

        /**
         * Cancel Process
         *
         * Stop processing queue items, clear cronjob and delete batch.
         *
         */
        public function cancel_process() {
            if ( !$this->is_queue_empty() ) {
                $batch = $this->get_batch();

                $this->delete( $batch->key );

                wp_clear_scheduled_hook( $this->cron_hook_identifier );
            }

        }

        /**
         * Task
         *
         * Override this method to perform any actions required on each
         * queue item. Return the modified item for further processing
         * in the next pass through. Or, return false to remove the
         * item from the queue.
         *
         * @param mixed $item Queue item to iterate over.
         *
         * @return mixed
         */
        abstract protected function task( $item, $batch_key );

    }
}