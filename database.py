import sqlite3

def create_tables():
    connection = sqlite3.connect('taxi_service.db')
    cursor = connection.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS User (
            userId TEXT PRIMARY KEY,
            card_num TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Coupon (
            couponId INTEGER PRIMARY KEY AUTOINCREMENT,
            userId TEXT,
            coupon BOOLEAN,
            FOREIGN KEY (userId) REFERENCES User (userId)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Rider (
            riderId TEXT PRIMARY KEY,
            car_num TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Drive (
            driveId INTEGER PRIMARY KEY AUTOINCREMENT,
            riderId TEXT,
            userId TEXT,
            pickupTime REAL,
            dropoffTime REAL,
            paymentState TEXT,
            FOREIGN KEY (riderId) REFERENCES Rider (riderId),
            FOREIGN KEY (userId) REFERENCES User (userId)
        )
    ''')

    connection.commit()
    connection.close()
