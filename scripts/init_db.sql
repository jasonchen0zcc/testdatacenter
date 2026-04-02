-- TDC 数据库初始化脚本
-- 基于最新设计：支持任务执行日志和数据标记关联

-- 任务执行日志表（必须先创建，被 tdc_data_tag 引用）
CREATE TABLE IF NOT EXISTS tdc_task_log (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL COMMENT '任务ID',
    task_name VARCHAR(128) NOT NULL COMMENT '任务名称',
    task_type ENUM('http_source', 'direct_insert') NOT NULL COMMENT '任务类型',
    status ENUM('running', 'success', 'failed', 'partial') NOT NULL COMMENT '执行状态',
    total_count INT UNSIGNED DEFAULT 0 COMMENT '总记录数',
    success_count INT UNSIGNED DEFAULT 0 COMMENT '成功记录数',
    failed_count INT UNSIGNED DEFAULT 0 COMMENT '失败记录数',
    error_msg TEXT COMMENT '错误信息',
    started_at DATETIME COMMENT '开始时间',
    finished_at DATETIME COMMENT '结束时间',
    INDEX idx_task_id (task_id),
    INDEX idx_status (status),
    INDEX idx_started_at (started_at)
) ENGINE=InnoDB COMMENT='任务执行日志';

-- 测试数据标记表（关联 task_log）
CREATE TABLE IF NOT EXISTS tdc_data_tag (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    task_log_id BIGINT UNSIGNED COMMENT '关联任务执行日志ID',
    user_id VARCHAR(64) NOT NULL COMMENT '业务用户ID',
    order_id VARCHAR(64) NOT NULL COMMENT '业务订单ID',
    data_tag VARCHAR(128) NOT NULL COMMENT '数据类型标签',
    task_id VARCHAR(64) NOT NULL COMMENT '任务ID，用于追溯',
    ext_info JSON COMMENT '扩展信息',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_task_log_id (task_log_id),
    INDEX idx_user_id (user_id),
    INDEX idx_order_id (order_id),
    INDEX idx_data_tag (data_tag),
    INDEX idx_created_at (created_at),
    FOREIGN KEY (task_log_id) REFERENCES tdc_task_log(id) ON DELETE SET NULL
) ENGINE=InnoDB COMMENT='测试数据标记表';
