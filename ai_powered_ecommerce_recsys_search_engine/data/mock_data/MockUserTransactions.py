import os
import random

import pandas as pd


os.chdir("../data_location")


class MockInputDataUserRatings:
    def __init__(self):
        self._columns = ['USER_ID', 'PRODUCT_ID', 'AMOUNT_PURCHASED']
        self._df = pd.DataFrame(columns=self._columns)
        self._df_products = pd.read_csv("walmart_full_data.csv")["Product Name"]
        self._number_of_users = 5000
        self._types_of_users = []
        self._number_of_products_per_user = []
        self._number_of_product_names = len(self._df_products.index)
        '''
        4 types of users
        '''
        self._number_of_active_users = 0
        self._number_of_mod_active_users = 0
        self._number_of_avg_users = 0
        self._number_of_inactive_users = 0

    def generate_users(self):
        for i in range(0, self._number_of_users):
            probability = random.random()
            self._types_of_users.append(probability)
        return self._types_of_users

    def generate_number_of_products_per_user(self):
        for i in range(0, len(self._types_of_users)):
            if self._types_of_users[i] < 0.1:
                activity = random.uniform(50, 70)
                number_of_products_purchased = activity/100 * self._number_of_product_names
                self._number_of_products_per_user.append(int(number_of_products_purchased))
            elif 0.1 <= self._types_of_users[i] < 0.3:
                activity = random.uniform(30, 50)
                number_of_products_purchased = activity / 100 * self._number_of_product_names
                self._number_of_products_per_user.append(int(number_of_products_purchased))
            elif 0.3 <= self._types_of_users[i] < 0.8:
                activity = random.uniform(15, 30)
                number_of_products_purchased = activity / 100 * self._number_of_product_names
                self._number_of_products_per_user.append(int(number_of_products_purchased))
            elif 0.8 <= self._types_of_users[i] < 1:
                activity = random.uniform(0, 15)
                number_of_products_purchased = activity / 100 * self._number_of_product_names
                self._number_of_products_per_user.append(int(number_of_products_purchased))
        return self._number_of_products_per_user

    def generate_final_dataframe(self):
        for i in range(0, self._number_of_users):
            print("Currently generating ratings for user id: ", i)
            purchase_frequencies = [0 for x in range(0, self._number_of_product_names)]
            target = self._number_of_products_per_user[i]
            target_products = random.sample(range(0, self._number_of_product_names-1), target)
            user_results = []
            for j in range(0, len(target_products)):
                purchase_frequencies[target_products[j]] = self.generate_rating()
                user_results.append([i, target_products[j], purchase_frequencies[target_products[j]]])
            temp = pd.DataFrame(user_results, columns=self._columns)
            self._df = self._df.append(temp)
        return self._df

    def generate_rating(self):
        threshold = random.random()
        if 0 <= threshold <= 0.5:
            return random.randint(1, 20)
        if 0.5 < threshold <= 0.7:
            return random.randint(20, 70)
        if 0.7 < threshold <= 0.9:
            return random.randint(70, 90)
        elif 0.9 < threshold <= 1:
            return random.randint(90, 100)

    def generate_statistics(self):
        for i in range(0, len(self._types_of_users)):
            if self._types_of_users[i] < 0.1:
                self._number_of_active_users += 1
            elif 0.1 <= self._types_of_users[i] < 0.3:
                self._number_of_mod_active_users += 1
            elif 0.3 <= self._types_of_users[i] < 0.8:
                self._number_of_avg_users += 1
            elif 0.8 <= self._types_of_users[i] < 1:
                self._number_of_inactive_users += 1

    def export(self):
        print("Currently exporting frequencies")
        self._df.to_csv("product_transactions_mock_data.csv", index=False)

    def visualize_statistics(self):
        print("Out of", self._number_of_users, "users:")
        print("Active Users:", self._number_of_active_users, ' - ', self._number_of_active_users/self._number_of_users *100, "%")
        print("Moderately Active Users:", self._number_of_mod_active_users, ' - ', self._number_of_mod_active_users/self._number_of_users * 100, "%")
        print("Average Users:", self._number_of_avg_users, ' - ', self._number_of_avg_users/self._number_of_users * 100, "%")
        print("Inactive Users:", self._number_of_inactive_users, ' - ', self._number_of_inactive_users/self._number_of_users * 100, "%")

    def execute_mock_pipeline(self):
        self.generate_users()
        self.generate_number_of_products_per_user()
        self.generate_final_dataframe()
        self.export()


mocker = MockInputDataUserRatings()
mocker.execute_mock_pipeline()