import psycopg2
import time
import random
from faker import Faker
from datetime import datetime, timedelta
import logging
from typing import Optional

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# database config
DB_CONFIG = {
    "dbname": "safebank",
    "user": "safebank",
    "password": "safebank",
    "host": "localhost",
    "port": "5433",
}

fake = Faker()


def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logging.error(f"Connection error: {e}")
        return None


def chaos_string(text: Optional[str]) -> Optional[str]:
    """
    Injects noise into strings: mixed case, trailing spaces.
    """
    if not text:
        return text

    choice = random.random()
    if choice < 0.3:
        return text.upper()
    elif choice < 0.6:
        return f"  {text}  "
    else:
        return text.lower()


def chaos_amount(amount: float) -> float:
    """
    Inject domain errors: negative amounts.
    """
    if random.random() < 0.05:
        return amount * -1
    return amount


def is_table_empty(conn, table_name: str) -> bool:
    """
    Checks if a table is empty.
    """
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cur.fetchone()[0]
    cur.close()

    return count == 0


# ========================================================
# STATIC DATA
# ========================================================
def create_branches(conn) -> None:
    """
    Seeds Branch table.
    """
    cur = conn.cursor()
    logging.info("Seeding Branches...")

    cities = [
        "Hai Phong",
        "Ho Chi Minh",
        "Ca Mau",
        "Gia Lai",
        "Dong Thap",
        "Can Tho",
        "Lam Dong",
        "An Giang",
        "Quang Ngai",
        "Quang Tri",
        "Hung Yen",
        "Cao Bang",
        "Lao Cai",
        "Ninh Binh",
        "Khanh Hoa",
        "Thai Nguyen",
        "Tay Ninh",
        "Son La",
        "Nghe An",
        "Phu Tho",
        "Hue",
        "Lang Son",
        "Quang Ninh",
        "Da Nang",
        "Ha Tinh",
        "Lai Chau",
        "Bac Ninh",
        "Dien Bien",
        "Dong Nai",
        "Tuyen Quang",
        "Vinh Long",
        "Thanh Hoa",
        "Dak Lak",
        "Ha Noi",
    ]

    for city in cities:
        branch_name = f"SafeBank {city}"
        open_date = fake.date_between(start_date="-10y", end_date="today")

        sql = """
            INSERT INTO branch (branch_name, city, open_date)
            VALUES (%s, %s, %s)
        """
        cur.execute(sql, (branch_name, city, open_date))

    conn.commit()
    cur.close()


def create_channels(conn):
    """
    Seeds Channels table.
    """
    cur = conn.cursor()
    logging.info("Seeding Channels...")
    channels = [
        ("APP", "Mobile Application"),
        ("WEB", "Internet Banking"),
        ("ATM", "Automated Teller Machine"),
        ("COUNTER", "Bank Counter"),
    ]

    for code, desc in channels:
        cur.execute(
            """
            INSERT INTO channel (channel_code, description)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """,
            (code, desc),
        )

    conn.commit()
    cur.close()


def create_currencies(conn):
    """
    Seeds Currency table.
    """
    cur = conn.cursor()
    logging.info("Seeding Currencies...")

    currencies = [
        ("VND", "Vietnam Dong"),
        ("USD", "US Dollar"),
        ("EUR", "Euro"),
        ("JPY", "Japanese Yen"),
        ("GBP", "British Pound"),
        ("AUD", "Australian Dollar"),
        ("CAD", "Canadian Dollar"),
        ("CHF", "Swiss Franc"),
        ("CNY", "Chinese Yuan"),
        ("SGD", "Singapore Dollar"),
        ("THB", "Thai Baht"),
        ("KRW", "South Korean Won"),
    ]

    for code, name in currencies:
        cur.execute(
            """
            INSERT INTO currency (code, name)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """,
            (code, name),
        )

    conn.commit()
    cur.close()


def create_exchange_rates(conn):
    """
    Seeds Currency table.
    """
    cur = conn.cursor()
    logging.info("Generating Exchange Rates history...")

    base_rates = {
        "VND": 1,
        "USD": 26375,
        "EUR": 29690,
        "JPY": 165.25,
        "GBP": 34961,
        "AUD": 17267,
        "CAD": 18939,
        "CHF": 28900,
        "CNY": 3550,
        "SGD": 19623,
        "THB": 750,
        "KRW": 18.5,
    }

    start_date = datetime.now() - timedelta(days=30)

    for i in range(31):
        current_date = start_date + timedelta(days=i)
        for curr, base_val in base_rates.items():
            if curr == "VND":
                continue

            variation = random.uniform(0.99, 1.01)
            rate = round(base_val * variation, 6)

            sql = """
                INSERT INTO exchange_rate (from_currency, to_currency, rate, effective_date)
                VALUES (%s, 'VND', %s, %s)
                ON CONFLICT (from_currency, to_currency, effective_date)
                DO UPDATE SET rate = EXCLUDED.rate
            """

            cur.execute(sql, (curr, rate, current_date.date()))

    conn.commit()
    cur.close()


def create_merchants(conn, count: int = 10):
    """
    Seeds Merchant table.
    """
    cur = conn.cursor()
    logging.info(f"Seeding {count} Merchants...")
    categories = [
        "Food & Beverage",
        "Transportation",
        "Entertainment",
        "Utilities",
        "Shopping",
    ]
    prefixes = ["Star", "Tech", "Viet", "Global", "Daily"]
    suffixes = ["Coffee", "Mart", "Taxi", "Cinema", "Power"]

    for _ in range(count):
        name = f"{random.choice(prefixes)} {random.choice(suffixes)}"
        category = random.choice(categories)
        cur.execute(
            """
            INSERT INTO merchant (name, category)
            VALUES (%s, %s)
        """,
            (name, category),
        )

    conn.commit()
    cur.close()


def create_devices(conn, count: int = 20):
    """
    Seeds Device table.
    """
    cur = conn.cursor()
    logging.info(f"Seeding {count} Devices...")

    model_os_map = {
        "iPhone 17": ["iOS 26"],
        "iPhone 16": ["iOS 26"],
        "Samsung S25": ["Android 16"],
        "Samsung Fold X": ["Android 16"],
        "Pixel 10": ["Android 16"],
        "Pixel Tablet": ["Android 16"],
        "Windows Chrome": ["Windows 11 v25H2"],
        "MacBook Safari": ["macOS Tahoe 26"],
        "iPad Air": ["iPadOS 26"],
        "iPad Pro": ["iPadOS 26"],
        "iMac": ["macOS Tahoe 26"],
        "Mac Studio": ["macOS Tahoe 26"],
    }

    models = list(model_os_map.keys())

    for _ in range(count):
        model = random.choice(models)
        os_ver = model_os_map[model]
        fingerprint = fake.uuid4()

        sql = """
            INSERT INTO device (device_fingerprint, device_model, os_version)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        cur.execute(sql, (fingerprint, model, os_ver))

    conn.commit()
    cur.close()


# ========================================================
# CORE ENTITIES
# ========================================================
def create_persons(conn, count: int = 10) -> None:
    """
    Seeds Person table with noisy data.
    """
    cur = conn.cursor()
    logging.info(f"Seeding {count} Persons (with noise)...")

    dirty_genders = [
        "Male",
        "Female",
        "M",
        "F",
        "m",
        "f",
        "Male ",
        "female",
        None,
        "Unknown",
    ]

    for _ in range(count):
        # dirty name
        raw_name = fake.name()
        name = chaos_string(raw_name)

        # dirty gender
        gender = random.choice(dirty_genders)

        # dirty birthday
        if random.random() < 0.05:
            birthday = (
                datetime(2099, 1, 1) if random.random() < 0.5 else datetime(1800, 1, 1)
            )
        else:
            birthday = fake.date_of_birth(minimum_age=18, maximum_age=90)

        # country
        country = fake.country()

        # city
        city = None if random.random() < 0.05 else chaos_string(fake.city())

        # fraud flag: 10% blocked
        is_blocked = random.choice([True] + [False] * 9)

        sql = """
            INSERT INTO person (name, gender, birthday, country, city, is_blocked)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        cur.execute(sql, (name, gender, birthday, country, city, is_blocked))

    conn.commit()
    cur.close()


def get_account_level(balance, currency):
    """
    Calculates account level based on balance converted to USD.
    """
    exchange_rates_to_usd = {
        "VND": 0.000038,
        "USD": 1.0,
        "EUR": 1.1257,
        "JPY": 0.0063,
        "GBP": 1.3255,
        "AUD": 0.6547,
        "CAD": 0.7181,
        "CHF": 1.0957,
        "CNY": 0.1346,
        "SGD": 0.7440,
        "THB": 0.0284,
        "KRW": 0.0007,
    }

    rate = exchange_rates_to_usd.get(currency, 1)
    usd_balance = rate * balance

    if usd_balance < 2000:
        return "Stardard"
    elif usd_balance < 10000:
        return "Silver"
    elif usd_balance < 50000:
        return "Gold"
    elif usd_balance < 200000:
        return "Platinum"
    else:
        return "Diamond"


def create_accounts(conn, count: int = 10) -> None:
    """
    Seeds Account table with noisy data.
    """
    cur = conn.cursor()
    logging.info(f"Seeding {count} Accounts (with noise)...")

    cur.execute(
        """
        SELECT id
        FROM person
        ORDER BY RANDOM()
        LIMIT %s
    """,
        (count,),
    )
    persons = cur.fetchall()

    cur.execute(
        """
        SELECT id
        FROM branch
    """
    )
    branch_ids = cur.fetchall()

    if not persons or not branch_ids:
        logging.warning("Missing Person or Branch data. Skipping account generation.")
        return

    dirty_types = [
        "checking",
        "saving",
        "CHECKING",
        "Saving",
        "sav",
        "chk",
        "vip",
        "VIP",
        "V.I.P",
        "Vip",
        "Business",
        "bussiness",
        "business",
    ]

    currencies = [
        "VND",
        "USD",
        "EUR",
        "JPY",
        "GBP",
        "AUD",
        "CAD",
        "CHF",
        "CNY",
        "SGD",
        "THB",
        "KRW",
    ]
    curr_weights = [70, 12, 5, 3, 2, 1.5, 1.5, 1, 2, 1, 0.5, 0.5]

    balance_config = {
        "VND": (50000.00, 5000000000.00),
        "USD": (50.00, 200000.00),
        "EUR": (50.00, 180000.00),
        "JPY": (5000.00, 25000000.00),
        "GBP": (40.00, 150000.00),
        "AUD": (70.00, 250000.00),
        "CAD": (70.00, 250000.00),
        "SGD": (70.00, 250000.00),
        "CHF": (50.00, 180000.00),
        "CNY": (300.00, 1500000.00),
        "THB": (1500.00, 7000000.00),
        "KRW": (50000.00, 250000000.00),
    }

    for person in persons:
        owner_id = person[0]
        branch_id = random.choice(branch_ids)[0]

        acc_type = random.choice(dirty_types)

        # balance
        curr_code = random.choices(currencies, weights=curr_weights, k=1)[0]

        min_bal, max_bal = balance_config.get(curr_code, (100.00, 100000.00))

        # 90% normal, 10% rich
        balance = (
            chaos_amount(round(random.uniform(min_bal, max_bal * 0.1), 2))
            if random.random() < 0.9
            else chaos_amount(round(random.uniform(min_bal, max_bal), 2))
        )

        if curr_code in ["VND", "JPY", "KRW"]:
            balance = round(balance, 0)

        # account level
        acc_level = get_account_level(balance, curr_code)

        # nickname
        nickname = fake.word()

        # dirty phone: replace format or inject chars
        raw_phone = fake.phone_number()[:20]
        phone = (
            raw_phone.replace("-", ".").replace("  ", "")
            if random.random() < 0.3
            else raw_phone
        )

        email = None if random.random() < 0.1 else fake.email()

        sql = """
            INSERT INTO account (owner_id, branch_id, account_type, account_level, currency_code, balance, nickname, phone_number, email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cur.execute(
            sql,
            (
                owner_id,
                branch_id,
                acc_type,
                acc_level,
                curr_code,
                balance,
                nickname,
                phone,
                email,
            ),
        )

    conn.commit()
    cur.close()


def create_loans(conn, count: int = 5):
    """
    Seeds Loan Account table.
    """
    cur = conn.cursor()
    logging.info(f"Seeding {count} Loans...")
    cur.execute(
        """
        SELECT id
        FROM account
        WHERE balance > 0
        ORDER BY RANDOM()
        LIMIT %s
    """,
        (count,),
    )
    acc_rows = cur.fetchall()
    if not acc_rows:
        return

    statuses = ["ACTIVE", "ACTIVE", "ACTIVE", "CLOSED", "DEFAULT"]
    currencies = [
        "VND",
        "USD",
        "EUR",
        "JPY",
        "GBP",
        "AUD",
        "CAD",
        "CHF",
        "CNY",
        "SGD",
        "THB",
        "KRW",
    ]
    loan_config = {
        "VND": (10000000.00, 2000000000.00),
        "USD": (1000.00, 100000.00),
        "EUR": (1000.00, 90000.00),
        "JPY": (100000.00, 10000000.00),
        "KRW": (1000000.00, 100000000.00),
        "GBP": (800.00, 80000.00),
        "AUD": (1500.00, 150000.00),
        "CAD": (1500.00, 150000.00),
        "CHF": (1000.00, 90000.00),
        "CNY": (7000.00, 700000.00),
        "SGD": (1500.00, 150000.00),
        "THB": (35000.00, 3500000.00),
    }

    for acc in acc_rows:
        acc_id = acc[0]

        curr_code = random.choice(currencies)
        min_val, max_val = loan_config.get(curr_code, (1000.00, 100000.00))
        amount = round(random.uniform(min_val, max_val), 2)

        if curr_code in ["VND", "JPY", "KRW"]:
            amount = round(amount, 0)

        rate = round(random.uniform(5.0, 12.0), 2)
        term = random.choice([12, 24, 36, 48, 60])
        start_date = fake.date_between(start_date="-2y", end_date="today")
        end_date = start_date + timedelta(days=term * 30)
        status = random.choice(statuses)

        remaining = amount
        if status == "CLOSED":
            remaining = 0
        elif status == "ACTIVE":
            remaining = amount * random.uniform(0.1, 0.9)
            remaining = (
                round(remaining, 0)
                if curr_code in ["VND", "JPY", "KRW"]
                else round(remaining, 2)
            )

        sql = """
            INSERT INTO loan_account (account_id, currency_code, amount, interest_rate, term_months, start_date, end_date, status, remaining_balance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(
            sql,
            (
                acc_id,
                curr_code,
                amount,
                rate,
                term,
                start_date,
                end_date,
                status,
                remaining,
            ),
        )

    conn.commit()
    cur.close()


# ========================================================
# TRANSACTIONS
# ========================================================
def generate_transfer(conn) -> None:
    """
    ACID compliant transfer transaction.
    1. lock sender and receiver rows
    2. check balance
    3. deduct from sender
    4. add to receiver if p2p
    5. insert transfer log
    6. commit or rollback
    """
    cur = conn.cursor()

    try:
        # select sender
        cur.execute(
            """
            SELECT id, currency_code
            FROM account
            ORDER BY RANDOM()
            LIMIT 1
        """
        )
        sender_row = cur.fetchone()

        if not sender_row:
            cur.close()
            return

        from_id, sender_curr = sender_row

        # set up participants
        is_payment = random.random() < 0.3
        to_id = None
        merchant_id = None

        if is_payment:
            cur.execute(
                """
                SELECT id
                FROM merchant
                ORDER BY RANDOM()
                LIMIT 1
            """
            )
            m_row = cur.fetchone()

            if m_row:
                merchant_id = m_row[0]
            comment = f"Payment Inv #{random.randint(1000,9999)}"
        else:
            cur.execute(
                """
                SELECT id
                FROM account
                WHERE id != %s
                ORDER BY RANDOM()
                LIMIT 1
            """,
                (from_id,),
            )
            acc_row = cur.fetchone()

            if acc_row:
                to_id = acc_row[0]

            comment = fake.sentence(nb_words=3)

        if not to_id and not merchant_id:
            cur.close()
            return

        # channel
        cur.execute(
            """
            SELECT id
            FROM channel
            ORDER BY RANDOM()
            LIMIT 1
        """
        )
        chan_row = cur.fetchone()
        channel_id = chan_row[0] if chan_row else 1

        # currency
        curr_code = sender_curr

        amount_config = {
            "VND": (50000.00, 100000000.00, 5000000000.00),
            "USD": (10.00, 5000.00, 100000.00),
            "EUR": (10.00, 4500.00, 90000.00),
            "GBP": (10.00, 4000.00, 80000.00),
            "CHF": (10.00, 4000.00, 80000.00),
            "AUD": (15.00, 7000.00, 150000.00),
            "CAD": (15.00, 7000.00, 150000.00),
            "SGD": (15.00, 7000.00, 150000.00),
            "CNY": (100.00, 30000.00, 500000.00),
            "THB": (500.00, 150000.00, 3000000.00),
            "JPY": (1000.00, 500000.00, 10000000.00),
            "KRW": (10000.00, 5000000.00, 100000000.00),
        }

        min_val, max_val, outlier_val = amount_config.get(
            curr_code, (10.00, 1000.00, 99999.00)
        )

        # noise: zero amount
        if random.random() < 0.02:
            amount = 0.00
        # noise: outlier - high value transaction
        elif random.random() < 0.01:
            amount = outlier_val
        # normal transaction
        else:
            amount = round(random.uniform(min_val, max_val), 2)
            if curr_code in ["VND", "JPY", "KRW"]:
                amount = round(amount, 0)

        # event time noise: late arrival data
        txn_time = (
            datetime.now() - timedelta(days=random.randint(1, 3))
            if random.random() < 0.05
            else datetime.now()
        )

        # ACID
        # - lock sender row
        cur.execute(
            """
            SELECT balance
            FROM account
            WHERE id = %s
            FOR UPDATE
        """,
            (from_id,),
        )

        current_balance = cur.fetchone()[0]
        current_balance = float(current_balance)

        # - check balance
        if amount > 0 and current_balance < amount:
            conn.rollback()
            logging.warning(
                f"[TRANSACTION SKIPPED] Acc {from_id}: Insufficient funds (Current Balance {current_balance} < Amount {amount})"
            )
            cur.close()
            return

        # - deduct from sender
        cur.execute(
            """
            UPDATE account
            SET balance = balance - %s, update_time = CURRENT_TIMESTAMP
            WHERE id = %s
        """,
            (amount, from_id),
        )

        # - add to receiver
        if to_id:
            # lock receiver row
            cur.execute(
                """
                SELECT id
                FROM account
                WHERE id = %s
                FOR UPDATE
            """,
                (to_id,),
            )

            cur.execute(
                """
                UPDATE account
                SET balance = balance + %s, update_time = CURRENT_TIMESTAMP
                WHERE id = %s
            """,
                (amount, to_id),
            )

        # - insert transfer log
        sql = """
            INSERT INTO transfer(from_account_id, to_account_id, merchant_id, amount, currency_code, channel_id, comment, txn_time)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
        """

        cur.execute(
            sql,
            (
                from_id,
                to_id,
                merchant_id,
                amount,
                curr_code,
                channel_id,
                comment,
                txn_time,
            ),
        )
        conn.commit()

        log_type = "[PAYMENT]" if is_payment else "[TRANSFER]"

        logging.info(
            f"{log_type} Account {from_id} -> {to_id or merchant_id}: -{amount:,.2f} {curr_code} | Balance Left: {current_balance - amount:,.2f}"
        )

    except Exception as e:
        logging.error(f"[TRANSACTION FAILED] Rolling back due to: {e}")
        conn.rollback()

    finally:
        cur.close()


def generate_sigin(conn):
    """
    Seeds Sign in table.
    """
    cur = conn.cursor()

    # account_id
    cur.execute(
        """
        SELECT id
        FROM account
        ORDER BY RANDOM()
        LIMIT 1
    """
    )
    acc_row = cur.fetchone()
    if not acc_row:
        return
    acc_id = acc_row[0]

    # device_id
    cur.execute(
        """
        SELECT id
        FROM device
        ORDER BY RANDOM()
        LIMIT 1
    """
    )
    dev_row = cur.fetchone()
    dev_id = dev_row[0] if dev_row else 1

    ip = fake.ipv4()
    city = fake.city()
    status = "FAILED" if random.random() < 0.1 else "SUCCESS"

    sql = """
        INSERT INTO sign_in (account_id, device_id, ip_address, location_city, status)
        VALUES (%s, %s, %s, %s, %s)
    """

    cur.execute(sql, (acc_id, dev_id, ip, city, status))
    conn.commit()
    if random.random() < 0.1:
        logging.info(f"[SignIn] Acc {acc_id} on Dev {dev_id} -> {status}")
    cur.close()


def generate_loan_payment(conn):
    """
    Seeds Loan Payment table.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, remaining_balance, currency_code
        FROM loan_account
        WHERE status = 'ACTIVE'
        ORDER BY RANDOM()
        LIMIT 1
    """
    )
    row = cur.fetchone()

    if row:
        loan_id, current_balance, curr_code = row
        current_balance = float(current_balance)

        if current_balance <= 0:
            return

        payment_amount = current_balance * random.uniform(0.05, 0.10)
        if curr_code in ["VND", "JPY", "KRW"]:
            payment_amount = round(payment_amount, 0)
        else:
            payment_amount = round(payment_amount, 2)

        late_days = random.randint(1, 30) if random.random() < 0.1 else 0
        payment_time = datetime.now()

        sql_pay = """
            INSERT INTO loan_payment (loan_id, amount, payment_date, late_days)
            VALUES (%s, %s, %s, %s)
        """
        cur.execute(sql_pay, (loan_id, payment_amount, payment_time, late_days))

        new_balance = max(0, current_balance - payment_amount)

        sql_update = """
            UPDATE loan_account
            SET remaining_balance = %s
            WHERE id = %s
        """
        cur.execute(sql_update, (new_balance, loan_id))
        conn.commit()

        logging.info(
            f"[LOAN PAYMENT] Loan {loan_id} paid {payment_amount:,.2f} {curr_code}"
        )

    cur.close()


# ========================================================
# UPDATE (SCD Type 2 Triggers)
# ========================================================
def update_person(conn):
    """
    Simlulates SCD Type 2: Person moves to a new city.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, city
        FROM person
        ORDER BY RANDOM()
        LIMIT 1
    """
    )
    row = cur.fetchone()

    if row:
        pid, old_city = row
        new_city = chaos_string(fake.city())

        sql = """
            UPDATE person
            SET city = %s, update_time = CURRENT_TIMESTAMP
            WHERE id = %s
        """

        cur.execute(sql, (new_city, pid))
        conn.commit()
        logging.info(f"[UPDATE SCD2] Person {pid} moved from {old_city} to {new_city}")

    cur.close()


def update_account(conn):
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, account_type, account_level, email, phone_number
        FROM account
        ORDER BY RANDOM()
        LIMIT 1
    """
    )

    row = cur.fetchone()

    if row:
        acc_id, old_type, old_level, old_email, old_phone = row

        # update type
        dirty_types = [
            "checking",
            "saving",
            "CHECKING",
            "Saving",
            "sav",
            "chk",
            "vip",
            "VIP",
            "V.I.P",
            "Vip",
            "Business",
            "bussiness",
            "business",
        ]

        new_type = random.choice(dirty_types) if random.random() < 0.2 else old_type

        # update email
        new_email = old_email
        if random.random() < 0.3:
            new_email = None if random.random() < 0.1 else fake.email()

        # update phone
        new_phone = old_phone
        if random.random() < 0.3:
            raw_phone = fake.phone_number()[:20]
            new_phone = (
                f"({raw_phone[:3]}) {raw_phone[3:]}"
                if random.random() < 0.5
                else chaos_string(raw_phone)
            )

        # update level
        levels = ["Standard", "Silver", "Gold", "Platinum", "VIP"]
        weights = [60, 25, 10, 4, 1]

        new_level = old_level
        if random.random() < 0.1:
            new_level = random.choices(levels, weights=weights, k=1)[0]

        if (
            new_type != old_type
            or new_level != old_level
            or new_email != old_email
            or new_phone != old_phone
        ):
            sql = """
                UPDATE account
                SET account_type = %s, account_level = %s, email = %s, phone_number = %s, update_time = CURRENT_TIMESTAMP
                WHERE id = %s
            """

            cur.execute(sql, (new_type, new_level, new_email, new_phone, acc_id))
            conn.commit()
            logging.info(f"[UPDATE ACCOUNT] ID {acc_id}: Info updated!")

    cur.close()


if __name__ == "__main__":
    conn = get_connection()

    if conn:
        print("STARTING DATA SEEDING...")
        # seed master data
        if is_table_empty(conn, "branch"):
            create_branches(conn)
        else:
            logging.info("Skipping Branch (already initialized)")

        if is_table_empty(conn, "channel"):
            create_channels(conn)
        else:
            logging.info("Skipping Channel (already initialized)")

        if is_table_empty(conn, "currency"):
            create_currencies(conn)
        else:
            logging.info("Skipping Currency (already initialized)")

        create_exchange_rates(conn)
        create_merchants(conn, 15)
        create_devices(conn, 25)

        # seed entities
        create_persons(conn, 50)
        create_accounts(conn, 80)
        create_loans(conn, 20)

        print("STARTING REAL-TIME SIMULATION...")
        try:
            while True:
                dice = random.random()

                if dice < 0.40:
                    generate_sigin(conn)
                elif dice < 0.75:
                    generate_transfer(conn)
                elif dice < 0.85:
                    generate_loan_payment(conn)
                elif dice < 0.92:
                    update_person(conn)
                else:
                    update_account(conn)

                time.sleep(random.uniform(0.2, 1.0))
        except KeyboardInterrupt:
            logging.info("Simulation Stopped.")
        finally:
            conn.close()
