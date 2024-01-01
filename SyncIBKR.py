import sys
from time import sleep
import requests
from ibflex import client, parser, FlexQueryResponse, BuySell
from datetime import datetime
import json


class SyncIBKR:

    def __init__(self, ghost_host, ibkrtoken, ibkrquery, ghost_token, ghost_currency, ibkr_category):
        self.ghost_token = ghost_token
        self.ghost_host = ghost_host
        self.ghost_currency = ghost_currency
        self.ibkrtoken = ibkrtoken
        self.ibkrquery = ibkrquery
        self.ibkr_category = ibkr_category

    def sync_ibkr(self):
        print("Fetching Query")
        response = client.download(self.ibkrtoken, self.ibkrquery)
        print("Parsing Query")
        query: FlexQueryResponse = parser.parse(response)
        activities = []
        date_format = "%Y-%m-%d"
        account_id = self.create_or_get_IBKR_accountId()
        if not account_id:
            print("Failed to retrieve account ID. Closing now.")
            return
        self.set_cash_to_account(account_id, self.get_cash_amount_from_flex(query, self.ghost_currency))
        for trade in query.FlexStatements[0].Trades:
            if trade.openCloseIndicator is None:
                print("Trade is not open or close (ignoring):", trade)
            elif trade.openCloseIndicator.CLOSE:
                date = datetime.strptime(str(trade.tradeDate), date_format)
                iso_format = date.isoformat()
                symbol = trade.symbol

                if not trade.currency or trade.currency == "":
                    print("Trade has no currency (ignoring):", trade)
                    continue
                else:
                    print("Trade has currency:", trade.currency)

                if trade.currency == "EUR":
                    continue

                symbol_mapping = {
                    ".USD-PAXOS": "USD",
                    "VUAA": ".L",
                    "ENGI": ".PA",
                    "ARRDd": "MT.AS",
                    "AKZA": ".AS",
                    "ALFEN": ".AS"
                }
                for key, value in symbol_mapping.items():
                    if key in trade.symbol:
                        symbol = trade.symbol.replace(key, "") + value
                        break

                if trade.buySell == BuySell.BUY:
                    buysell = "BUY"
                elif trade.buySell == BuySell.SELL:
                    buysell = "SELL"
                else:
                    print("Trade is not buy or sell (ignoring):", trade)
                    continue

                activities.append({
                    "accountId": account_id,
                    "comment": None,
                    "currency": trade.currency,
                    "dataSource": "YAHOO",
                    "date": iso_format,
                    "fee": float(0),
                    "quantity": abs(float(trade.quantity)),
                    "symbol": symbol.replace(" ", "-"),
                    "type": buysell,
                    "unitPrice": float(trade.tradePrice)
                })

        diff = self.get_diff(self.get_all_acts_for_account(account_id), activities)
        if len(diff) == 0:
            print("Nothing new to sync")
        else:
            self.import_act(diff)

    def get_cash_amount_from_flex(self, query, currency="EUR"):
        cash = 0
        try:
            for item in query.FlexStatements[0].CashReport:
            # Check if the currency field exists and is equal to 'EUR'
                if hasattr(item, 'currency') and item.currency == currency:
                    cash += item.endingCash
                    print(cash)
            #cash += query.FlexStatements[0].CashReport[0].endingCash
        except Exception as e:
            print(e)
        try:
            print("Trying to get cash from paxos")
            cash += query.FlexStatements[0].CashReport[0].endingCashPaxos
        except Exception as e:
            print(e)
        return cash

    def get_diff(self, old_acts, new_acts):
        return [new_act for new_act in new_acts if new_act not in old_acts]

    def set_cash_to_account(self, account_id, cash):
        if cash == 0:
            print("No cash set, no cash retrieved")
            return False
        print(cash)
        
        account = {
            "balance": float(cash),
            "id": account_id,
            "currency": self.ghost_currency,
            "isExcluded": False,
            "name": "IBKR",
            "platformId": self.ibkr_category
        }

        url = f"{self.ghost_host}/api/v1/account/{account_id}"

        payload = json.dumps(account)
        headers = {
            'Authorization': f"Bearer {self.ghost_token}",
            'Content-Type': 'application/json'
        }
        try:
            response = requests.put(url, headers=headers, data=payload)
        except Exception as e:
            print(e)
            return False
        if response.status_code == 200:
            print(f"Updated Cash for account {response.json()['id']}")
        else:
            print("Failed create:", response.text)
        return response.status_code == 200

    def delete_act(self, act_id):
        url = f"{self.ghost_host}/api/v1/order/{act_id}"

        headers = {
            'Authorization': f"Bearer {self.ghost_token}",
        }
        try:
            response = requests.delete(url, headers=headers)
        except Exception as e:
            print(e)
            return False

        return response.status_code == 200

    def import_act(self, bulk):
        chunks = self.generate_chunks(bulk, 10)
        for acts in chunks:
            url = f"{self.ghost_host}/api/v1/import"
            formatted_acts = json.dumps({"activities": sorted(acts, key=lambda x: x["date"])})
            payload = formatted_acts
            headers = {
                'Authorization': f"Bearer {self.ghost_token}",
                'Content-Type': 'application/json'
            }
            print("Adding activities:", payload)
            try:
                response = requests.post(url, headers=headers, data=payload)
            except Exception as e:
                print(e)
                continue
            if response.status_code == 201:
                print(f"Created {payload}")
            else:
                print("Failed create:", response.text)
                print("**********************")
                print(payload)
            if response.status_code != 201:
                return False
        return True

    def generate_chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def create_ibkr_account(self):
        print("Try creating account")
        account = {
            "balance": 0,
            "currency": self.ghost_currency,
            "isExcluded": False,
            "name": "IBKR",
            "platformId": self.IBKRCATEGORY
        }

        url = f"{self.ghost_host}/api/v1/account"

        payload = json.dumps(account)
        headers = {
            'Authorization': f"Bearer {self.ghost_token}",
            'Content-Type': 'application/json'
        }
        try:
            response = requests.post(url, headers=headers, data=payload)
        except Exception as e:
            print(e)
            return ""
        if response.status_code == 201:
            
            return response.json()["id"]
        print(response.json())
        print("Failed creating account")
        return ""

    def get_account(self):
        url = f"{self.ghost_host}/api/v1/account"

        headers = {
            'Authorization': f"Bearer {self.ghost_token}",
        }
        try:
            response = requests.get(url, headers=headers)
        except Exception as e:
            print(e)
            return []
        if response.status_code == 200:
            return response.json()['accounts']
        else:
            raise Exception(response)

    def create_or_get_IBKR_accountId(self):
        accounts = self.get_account()
        for account in accounts:
            if account["name"] == "IBKR":
                print(f"Found IBKR account: {account['id']} and currency {account['currency']} ")
                return account["id"]
        return self.create_ibkr_account()

    def delete_all_acts(self):
        account_id = self.create_or_get_IBKR_accountId()
        acts = self.get_all_acts_for_account(account_id)

        if not acts:
            print("No activities to delete")
            return True
        complete = True

        for act in acts:
            if act['accountId'] == account_id:
                act_complete = self.delete_act(act['id'])
                complete = complete and act_complete
                if act_complete:
                    print("Deleted:", act['id'])
                else:
                    print("Failed Delete:", act['id'])
        return complete

    def get_all_acts_for_account(self, account_id):
        acts = self.get_all_acts()
        return [act for act in acts if act['accountId'] == account_id]

    def get_all_acts(self):
        url = f"{self.ghost_host}/api/v1/order"

        headers = {
            'Authorization': f"Bearer {self.ghost_token}",
        }
        try:
            response = requests.get(url, headers=headers)
        except Exception as e:
            print(e)
            return []

        return response.json()['activities'] if response.status_code == 200 else []
