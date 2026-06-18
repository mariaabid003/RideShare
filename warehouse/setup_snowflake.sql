-- Phase 4 - one-time Snowflake setup.
-- Run this in a Snowflake worksheet (as ACCOUNTADMIN or a role that can create
-- warehouses/databases) BEFORE running the loader and dbt.

CREATE WAREHOUSE IF NOT EXISTS RIDESHARE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60          -- suspend after 60s idle to save credits
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS RIDESHARE;

USE DATABASE RIDESHARE;

-- RAW   : landing zone the Python loader writes to
-- GOLD  : clean, modeled tables that dbt builds
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS GOLD;
