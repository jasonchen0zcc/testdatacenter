CREATE TABLE IF NOT EXISTS tdc_data_tag (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL COMMENT '业务用户ID',
    order_id VARCHAR(64) NOT NULL COMMENT '业务订单ID',
    data_tag VARCHAR(128) NOT NULL COMMENT '数据类型标签',
    task_id VARCHAR(64) NOT NULL COMMENT '任务ID，用于追溯',
    ext_info JSON COMMENT '扩展信息',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_order_id (order_id),
    INDEX idx_data_tag (data_tag),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB COMMENT='测试数据标记表';

CREATE TABLE IF NOT EXISTS tdc_task_log (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    task_name VARCHAR(128) NOT NULL,
    task_type ENUM('http_source', 'direct_insert') NOT NULL,
    status ENUM('running', 'success', 'failed', 'partial') NOT NULL,
    total_count INT UNSIGNED DEFAULT 0,
    success_count INT UNSIGNED DEFAULT 0,
    failed_count INT UNSIGNED DEFAULT 0,
    error_msg TEXT,
    started_at DATETIME,
    finished_at DATETIME,
    INDEX idx_task_id (task_id),
    INDEX idx_status (status),
    INDEX idx_started_at (started_at)
) ENGINE=InnoDB COMMENT='任务执行日志';
