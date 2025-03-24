import sqlite3
import re
import time

def extract_issuer(row, issuers_set) -> (str, str):
    # returns the issuer and the date of the press release
    # check if the row starts with a word in a set issuers

    for issuer in issuers_set:
        if row[4].startswith(issuer):
            return issuer
    raise ValueError(f"issuer not found in row {row}")

conn = sqlite3.connect('press_releases_02.db')
cursor = conn.cursor()
update_cursor = conn.cursor()

# alter the table to add the issuer column if the column does not exist
try:
    cursor.execute('ALTER TABLE press_releases ADD COLUMN issuer TEXT')
except sqlite3.OperationalError:
    # the column already exists
    pass

cursor.execute('SELECT * FROM press_releases')

issuers_set = {"Altro", "Consiglio", "Giunta", "Notizie di servizio", "Quartieri", "Sindaco"}
for row in cursor:
    issuer = extract_issuer(row, issuers_set)
    # remove the issuer and the date from the content, +1 for the space
    len_issuer = len(issuer)+1
    len_date = 11
    total_len = len_issuer + len_date
    row_content = row[4][total_len:]
    # print(f"{issuer}: {row_content}")
    # update the row
    update_cursor.execute('UPDATE press_releases SET issuer = ? WHERE id = ?', (issuer, row[0]))
    update_cursor.execute('UPDATE press_releases SET content = ? WHERE id = ?', (row_content, row[0]))

conn.commit()