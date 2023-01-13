import sqlite3
connection = sqlite3.connect('crud.db')

cursor = connection.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS Links
(url_id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT)
''')
#urlsToInsert = [
#    ('http://kaspi.kz')
#]
#cursor.execute('''
#INSERT INTO Links(url) VALUES(?)
#''', urlsToInsert)
cursor.execute("SELECT count(*) from Links")
print(cursor.fetchall())


connection.commit()
connection.close()

