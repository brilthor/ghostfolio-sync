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
        """
        Synchronizes the IBKR activities with the Ghostfolio platform.

        Fetches the IBKR query, parses it, and retrieves the cash amount for each IBKR account.
        Sets the cash amount to the corresponding Ghostfolio account.
        Processes the trades from the IBKR query and converts them into Ghostfolio activities.
        Imports the new activities into the Ghostfolio platform.

        """

        print("Fetching Query")
        response = client.download(self.ibkrtoken, self.ibkrquery)
        print("Parsing Query")
        query: FlexQueryResponse = parser.parse(response)
        activities = []
        date_format = "%Y-%m-%d"
        ibkr_accounts = self.create_or_get_IBKR_accountId()

        # Check if the returned value is a dictionary
        if isinstance(ibkr_accounts, dict):
            for account_id, currency in ibkr_accounts.items():
                cash_amount = self.get_cash_amount_from_flex(query, currency)
                self.set_cash_to_account(account_id, cash_amount, currency)
        # Check if the returned value is an integer (single account ID)
        elif isinstance(ibkr_accounts, int):
            account_id = ibkr_accounts
            currency = self.ghost_currency  # Assuming the single account uses this currency
            cash_amount = self.get_cash_amount_from_flex(query, currency)
            self.set_cash_to_account(account_id, cash_amount)
        else:
            print("Failed to retrieve IBKR accounts. Closing now.")
            return

        for trade in query.FlexStatements[0].Trades:
            if trade.openCloseIndicator is None or trade.assetCategory == "CASH":
                continue
            elif trade.openCloseIndicator.CLOSE:
                date = datetime.strptime(str(trade.tradeDate), date_format)
                iso_format = date.isoformat()
                symbol = trade.symbol

                account_id = self.get_account_id_for_currency(ibkr_accounts, trade.currency)
                
                if not account_id:
                    print(f"No account found for currency {trade.currency}. Skipping this trade.")
                    continue

                if not trade.currency or trade.currency == "":
                    print("Trade has no currency (ignoring):", trade)
                    continue

                if trade.currency == "EUR":
                    currency = str("EUR")
                else:
                    currency = trade.currency

                symbol_mapping = {
                    ".USD-PAXOS": "USD",
                    "VUAA": ".L",
                    "ENGI": "ENGI.PA",
                    "ARRDd": "MT.AS",
                    "AKZA": "AKZA.AS",
                    "ALFEN": "ALFEN.AS"
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

                print(f"Adding {buysell} {trade.quantity} {trade.symbol} to {account_id}")

                try:
                    activities.append({
                        "accountId": account_id,
                        "comment": None,
                        "currency": currency,
                        "dataSource": "YAHOO",
                        "date": iso_format,
                        "fee": float(0),
                        "quantity": abs(float(trade.quantity)),
                        "symbol": symbol.replace(" ", "-"),
                        "type": buysell,
                        "unitPrice": float(trade.tradePrice)
                    })
                except Exception as e:
                    print(e)
                    
        #diff = self.get_diff(self.get_all_acts_for_account(account_id), activities)
        for account_id, currency in ibkr_accounts.items():
            # Filter activities based on currency
            currency_activities = [act for act in activities if act['currency'] == currency]
            
            # Get the old activities for the current account_id
            old_acts = self.get_all_acts_for_account(account_id)
            
            # Calculate the diff for the current currency and account_id
            diff = self.get_diff(old_acts, currency_activities)
            if currency == "EUR":
                print("EUR")
                print(diff)
            
            if len(diff) == 0:
                print("Nothing new to sync")
            else:
                
                self.import_act(diff)

    def get_account_id_for_currency(self, ibkr_accounts, trade_currency):
        """
        Returns the account ID associated with the given trade currency from the IBKR accounts.

        Args:
            ibkr_accounts: The IBKR accounts, which can be a dictionary or an integer.
            trade_currency: The currency of the trade.

        Returns:
            The account ID associated with the given trade currency from the IBKR accounts,
            or None if no matching account is found.

        """
        if isinstance(ibkr_accounts, dict):
            for account_id, currency in ibkr_accounts.items():
                if currency == trade_currency:
                    return account_id
        elif isinstance(ibkr_accounts, int):
            return ibkr_accounts
        return None

    def get_cash_amount_from_flex(self, query, currency="EUR"):
        """
        Returns the total cash amount in the specified currency from the Flex query.

        Args:
            query: The Flex query object.
            currency: The currency to filter the cash amounts (default is "EUR").

        Returns:
            The total cash amount in the specified currency from the Flex query.

        """

        cash = 0
        try:
            for item in query.FlexStatements[0].CashReport:
            # Check if the currency field exists and is equal to 'EUR'
                if hasattr(item, 'currency') and item.currency == currency:
                    cash += item.endingCash
        except Exception as e:
            print(e)

        return cash

    def get_diff(self, old_acts, new_acts):
        """
        Returns a list of new activities that are not present in the old activities.

        Args:
            old_acts: A list of old activities.
            new_acts: A list of new activities.

        Returns:
            A list of new activities that are not present in the old activities.

        """

        return [new_act for new_act in new_acts if new_act not in old_acts]

    def set_cash_to_account(self, account_id, cash, currency="USD"):
        if cash == 0:
            print("No cash set, no cash retrieved")
            return False
        print(cash)
        
        account = {
            "balance": float(cash),
            "id": account_id,
            "currency": currency,
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
        
        ibkr_accounts = {account["id"]: account["currency"] for account in accounts if account["name"] == "IBKR"}
        if len(ibkr_accounts) > 1:
            print("Multiple IBKR accounts found")
            print(ibkr_accounts)
        else:
            return self.create_ibkr_account()
        
        return ibkr_accounts
        
    def delete_all_acts(self):
        account_id = self.create_or_get_IBKR_accountId()
        acts = []
        if isinstance(account_id, dict):
            for id, currency in account_id.items():
                acts.extend(self.get_all_acts_for_account(id))
        elif isinstance(account_id, int):
            acts = self.get_all_acts_for_account(account_id)

        if not acts:
            print("No activities to delete")
            return True
        complete = True
        #print(len(acts))
        #sys.exit()

        for act in acts:
            #print("Trying to delete act")
           #print(act)
            act_complete = self.delete_act(act['id'])
            complete = complete and act_complete
            
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
