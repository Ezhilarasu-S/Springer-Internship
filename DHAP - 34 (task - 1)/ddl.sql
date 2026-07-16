-- ============================================================================
-- DDL for email_communications table
-- Dataset: email_communications
-- Purpose: Store processed email communication records from Aetheros platform
-- Created: 2026-07-15
-- ============================================================================

-- Drop table if it exists (idempotent)
DROP TABLE IF EXISTS email_communications CASCADE;

-- Create the main table
CREATE TABLE email_communications (
    id SERIAL PRIMARY KEY,
    subject VARCHAR(500) NOT NULL,
    sender VARCHAR(255) NOT NULL,
    receiver VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    message_body TEXT NOT NULL,
    thread_id VARCHAR(100) NOT NULL,
    email_types VARCHAR(50) NOT NULL,
    email_status VARCHAR(50) NOT NULL,
    email_criticality VARCHAR(20) NOT NULL,
    product_types VARCHAR(255) NOT NULL,
    agent_effectivity VARCHAR(50),
    agent_efficiency VARCHAR(50),
    customer_satisfaction NUMERIC(5, 4),
    loaded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT email_status_check 
        CHECK (email_status IN ('ongoing', 'completed')),
    
    CONSTRAINT email_criticality_check 
        CHECK (email_criticality IN ('low', 'medium', 'high')),
    
    CONSTRAINT customer_satisfaction_range 
        CHECK (customer_satisfaction >= -1.0 AND customer_satisfaction <= 1.0),
    
    -- Unique constraint on natural key to prevent duplicates
    UNIQUE(thread_id, sender, receiver, timestamp)
);

-- Create indexes for common query patterns
CREATE INDEX idx_email_communications_thread_id 
    ON email_communications(thread_id);

CREATE INDEX idx_email_communications_email_status 
    ON email_communications(email_status);

CREATE INDEX idx_email_communications_timestamp 
    ON email_communications(timestamp DESC);

CREATE INDEX idx_email_communications_sender 
    ON email_communications(sender);

CREATE INDEX idx_email_communications_receiver 
    ON email_communications(receiver);

CREATE INDEX idx_email_communications_email_criticality 
    ON email_communications(email_criticality);

CREATE INDEX idx_email_communications_loaded_at 
    ON email_communications(loaded_at);

-- Add table comment
COMMENT ON TABLE email_communications IS 
    'Processed email communications from Aetheros customer support platform. 
     Used for analytics, tracking, and customer satisfaction monitoring.';

-- Column comments
COMMENT ON COLUMN email_communications.thread_id IS 'Unique identifier for email conversation thread';
COMMENT ON COLUMN email_communications.email_status IS 'Current resolution status: ongoing or completed';
COMMENT ON COLUMN email_communications.email_criticality IS 'Priority classification: low, medium, or high';
COMMENT ON COLUMN email_communications.customer_satisfaction IS 'Customer satisfaction score from 0.0 to 1.0';
COMMENT ON COLUMN email_communications.loaded_at IS 'Timestamp when record was inserted into this table';

-- ============================================================================
-- Verification
-- ============================================================================
-- Run this query to verify the table was created:
-- SELECT * FROM information_schema.tables WHERE table_name = 'email_communications';
