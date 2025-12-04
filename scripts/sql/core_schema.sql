-- ========================================================
-- 1. CLEAN UP
-- ========================================================
DROP TABLE IF EXISTS branch CASCADE;
DROP TABLE IF EXISTS currency CASCADE;
DROP TABLE IF EXISTS channel CASCADE;
DROP TABLE IF EXISTS merchant CASCADE;
DROP TABLE IF EXISTS device CASCADE;
DROP TABLE IF EXISTS person CASCADE;
DROP TABLE IF EXISTS account CASCADE;

DROP TABLE IF EXISTS transfer CASCADE;
DROP TABLE IF EXISTS sign_in CASCADE;
DROP TABLE IF EXISTS loan_payment CASCADE;
DROP TABLE IF EXISTS exchange_rate CASCADE;

DROP TABLE IF EXISTS loan_account CASCADE;


-- ========================================================
-- 2. DIMENSIONS
-- ========================================================
CREATE TABLE branch (
    id SERIAL PRIMARY KEY,
    branch_name VARCHAR(100),
    city VARCHAR(100),
    open_date DATE
);

CREATE TABLE currency (
    code VARCHAR(3) PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE channel (
    id SERIAL PRIMARY KEY,
    channel_code VARCHAR(20) UNIQUE,
    description VARCHAR(100)
);

CREATE TABLE merchant (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50)
);

CREATE TABLE device (
    id SERIAL PRIMARY KEY,
    device_fingerprint VARCHAR(100) UNIQUE,
    device_model VARCHAR(100),
    os_version VARCHAR(50),
    is_trusted BOOLEAN DEFAULT TRUE
);

CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    gender VARCHAR(10),
    birthday DATE,
    country VARCHAR(100),
    city VARCHAR(100),
    is_blocked BOOLEAN DEFAULT FALSE,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE account (
    id SERIAL PRIMARY KEY,
    owner_id INT REFERENCES person(id),
    branch_id INT REFERENCES branch(id),
    account_type VARCHAR(20),
    account_level VARCHAR(20),
    currency_code VARCHAR(3) REFERENCES currency(code) DEFAULT 'VND',
    balance DECIMAL(15, 2) DEFAULT 0,
    nickname VARCHAR(100),
    phone_number VARCHAR(50),
    email VARCHAR(100),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_blocked BOOLEAN DEFAULT FALSE
);

CREATE TABLE loan_account (
    id SERIAL PRIMARY KEY,
    account_id INT REFERENCES account(id),
    currency_code VARCHAR(3) REFERENCES currency(code) DEFAULT 'VND',
    amount DECIMAL(15, 2),
    interest_rate DECIMAL(5, 2),
    term_months INT,
    start_date DATE,
    end_date DATE,
    status VARCHAR(20),
    remaining_balance DECIMAL(15, 2)
);


-- ========================================================
-- 3. FACTS / EVENTS
-- ========================================================
CREATE TABLE exchange_rate (
    id SERIAL PRIMARY KEY,
    from_currency VARCHAR(3) REFERENCES currency(code),
    to_currency VARCHAR(3) REFERENCES currency(code),
    rate DECIMAL(15, 6),
    effective_date DATE,
    UNIQUE(from_currency, to_currency, effective_date)
);

CREATE TABLE transfer (
    id SERIAL PRIMARY KEY,
    txn_time TIMESTAMP NOT NULL,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    from_account_id INT REFERENCES account(id),
    to_account_id INT REFERENCES account(id), -- NULL IF MERCHANT
    merchant_id INT REFERENCES merchant(id), -- NULL IF P2P

    amount DECIMAL(15, 2),
    currency_code VARCHAR(3) REFERENCES currency(code) DEFAULT 'VND',

    channel_id INT REFERENCES channel(id),
    comment VARCHAR(255),
    status VARCHAR(20) DEFAULT 'SUCCESS'
);

CREATE TABLE sign_in (
    id SERIAL PRIMARY KEY,
    account_id INT REFERENCES account(id),
    device_id INT REFERENCES device(id),
    sign_in_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ip_address VARCHAR(45),
    location_city VARCHAR(100),
    status VARCHAR(20)
);

CREATE TABLE loan_payment (
    id SERIAL PRIMARY KEY,
    loan_id INT REFERENCES loan_account(id),
    amount DECIMAL(15, 2),
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    late_days INT DEFAULT 0
);

-- ========================================================
-- CDC AND INDEXING
-- ========================================================
-- 1. cdc configuration (scd type 2)
ALTER TABLE person REPLICA IDENTITY FULL;
ALTER TABLE account REPLICA IDENTITY FULL;
ALTER TABLE transfer REPLICA IDENTITY FULL;
ALTER TABLE loan_account REPLICA IDENTITY FULL;

-- 2. indexing strategy
-- date/time
CREATE INDEX idx_person_update ON person(update_time);
CREATE INDEX idx_transfer_time ON transfer(txn_time);
CREATE INDEX idx_transfer_create ON transfer(create_time);
CREATE INDEX idx_signin_time ON sign_in(sign_in_time);

-- foreign key
CREATE INDEX idx_account_owner ON account(owner_id);
CREATE INDEX idx_account_branch ON account(branch_id);

CREATE INDEX idx_transfer_from ON transfer(from_account_id);
CREATE INDEX idx_transfer_to ON transfer(to_account_id);
CREATE INDEX idx_transfer_merchant ON transfer(merchant_id);

CREATE INDEX idx_loan_acc ON loan_account(account_id);
CREATE INDEX idx_loan_pay_loan ON loan_payment(loan_id);

CREATE INDEX idx_signin_acc ON sign_in(account_id);
CREATE INDEX idx_signin_dev ON sign_in(device_id);
