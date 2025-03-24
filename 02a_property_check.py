import sqlite3
import re

def properties_hold(row):
    
    starts_with_word_then_space = re.match(r"\w+\s", row[4])
    has_date_after_starting_word = re.match(r"\d{2}\/\d{2}\/\d{4}", row[4].split()[1])
    return starts_with_word_then_space and has_date_after_starting_word

conn = sqlite3.connect('press_releases_copy.db')
cursor = conn.cursor()

cursor.execute('SELECT * FROM press_releases')
violating_rows = []
starting_words_set = set()
for row in cursor:
    if not properties_hold(row):
        violating_rows.append(row)
    starting_words_set.add(row[4].split()[0])

print(f"there are {len(violating_rows)} rows that violate the property")

print(f"there are {len(starting_words_set)} unique starting words")
print(starting_words_set)

# dump the violating rows to a file
with open('violating_rows.txt', 'w') as f:
    for row in violating_rows:
        f.write(f"{row}\n")