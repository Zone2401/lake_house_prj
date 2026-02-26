"""
users_info.py - Generate fake user data

Returns: DataFrame with n rows
"""

import pandas as pd
from faker import Faker

fake = Faker('en_US')


def get_users_data(n=1000):
    """Generate n rows of fake user data.

    Columns: id, name, email, phone, address, created_date, amount, status
    Returns a DataFrame.
    """
    print(f"Generating {n} rows of user data...")

    rows = []
    for i in range(1, n + 1):
        rows.append({
            'id':           i,
            'name':         fake.name(),
            'email':        fake.email(),
            'phone':        fake.phone_number(),
            'address':      fake.address(),
            'created_date': fake.date_time_this_year(),
            'amount':       round(fake.random.uniform(100, 10000), 2),
            'status':       fake.random_element(['active', 'inactive', 'pending']),
        })

    df = pd.DataFrame(rows)
    print(f"  -> {len(df)} rows generated")
    return df