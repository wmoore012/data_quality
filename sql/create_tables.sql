-- SPDX-License-Identifier: MIT
-- Copyright (c) 2024 MusicScope

-- Data Quality Module Tables
-- Module-specific tables for validation rules and results

-- Quality validation rules table
CREATE TABLE IF NOT EXISTS data_quality_rules (
    rule_id VARCHAR(100) PRIMARY KEY,
    rule_name VARCHAR(200) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    rule_type ENUM('nulls', 'duplicate', 'orphan', 'schema', 'custom') NOT NULL,
    severity ENUM('critical', 'warning', 'info') NOT NULL DEFAULT 'warning',
    sql_check TEXT NOT NULL,
    description TEXT,
    owner VARCHAR(100) DEFAULT 'data_team',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_table_active (table_name, is_active),
    INDEX idx_severity (severity),
    INDEX idx_rule_type (rule_type)
) ENGINE=InnoDB COMMENT='Data quality validation rules';

-- Quality scan results table
CREATE TABLE IF NOT EXISTS data_quality_results (
    result_id VARCHAR(100) PRIMARY KEY,
    scan_timestamp TIMESTAMP NOT NULL,
    rule_id VARCHAR(100) NOT NULL,
    issue_count INT NOT NULL DEFAULT 0,
    severity ENUM('critical', 'warning', 'info') NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    details JSON,
    scan_duration_ms INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (rule_id) REFERENCES data_quality_rules(rule_id),
    INDEX idx_scan_timestamp (scan_timestamp DESC),
    INDEX idx_table_severity (table_name, severity),
    INDEX idx_rule_results (rule_id, scan_timestamp DESC)
) ENGINE=InnoDB COMMENT='Data quality scan results';

-- Quality thresholds table
CREATE TABLE IF NOT EXISTS data_quality_thresholds (
    threshold_id VARCHAR(100) PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    warning_threshold DECIMAL(10,4),
    critical_threshold DECIMAL(10,4),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_table_metric (table_name, metric_name),
    INDEX idx_table_active (table_name, is_active)
) ENGINE=InnoDB COMMENT='Quality metric thresholds';