import random
import string
import pandas as pd

number_of_users = 10000

'''
Data about the user:
-username
-first name
-last name
-email
-is_staff
-is_active
-date_joined
'''


class User:
    def __init__(self, id, username, first_name, last_name, email, date_joined):
        self.id = id
        self.username = username
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.is_staff = False
        self.is_active = True
        self.date_joined = date_joined

    def to_dict(self):
        return {
            'USER_ID': self.id,
            'USERNAME': self.username,
            'FIRST_NAME': self.first_name,
            'LAST_NAME': self.last_name,
            'EMAIL': self.email,
            'DATE_JOINED': self.date_joined
        }


def generate_username():
    return "username" + str(random.randint(0, 100000)) + random.choice(string.ascii_letters)


def generate_password():
    return "password" + str(random.randint(0, 100000)) + random.choice(string.ascii_letters)


def generate_first_name():
    return random.choice(string.ascii_letters) + random.choice(string.ascii_letters) + random.choice(string.ascii_letters)


def generate_last_name():
    return random.choice(string.ascii_letters) + random.choice(string.ascii_letters) + random.choice(string.ascii_letters)


def generate_email(firstname, lastname):
    return firstname + '_' + lastname + '@mock.com'


def generate_date_joined():
    return random.randint(1, 30) + random.randint(1, 12) + 2019


user_names = []
users = []
for i in range(0, number_of_users):
    username = generate_username()
    while username in user_names:
        username = generate_username()
    first_name = generate_first_name()
    last_name = generate_last_name()
    users.append(User(i, username, first_name, last_name, generate_email(first_name, last_name), generate_date_joined()))


df_users = pd.DataFrame.from_records([user.to_dict() for user in users])
df_users.to_csv('data/Users.csv', index=False)


