-- Migration: Add retry_count to pending_comments
-- Ensures robust retry and escalation logic for the queue.
ALTER TABLE pending_comments ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;

