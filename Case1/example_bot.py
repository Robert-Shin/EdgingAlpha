from typing import Optional

import xchange_client
import asyncio
import argparse


class MyXchangeClient(xchange_client.XChangeClient):

    def __init__(self, host: str, username: str, password: str):
        super().__init__(host, username, password)
        self.signatures = []
        self.uns_tstmps = []
        self.transaction_history = {"AKAV": {}, "MKJ": {}, "AKIM": {}, "DLR": {}, "APT": {}}

    async def bot_handle_cancel_response(self, order_id: str, success: bool, error: Optional[str]) -> None:
        order = self.open_orders[order_id]
        print(f"{'Market' if order[2] else 'Limit'} Order ID {order_id} cancelled, {order[1]} unfilled")

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int):
        print("order fill", self.positions)

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        print("order rejected because of ", reason)


    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
        #print("Trade has occurred")
        pass
        #self.transaction_history[symbol][timestamp] = (price, qty)



    async def bot_handle_book_update(self, symbol: str) -> None:
        pass

    async def bot_handle_swap_response(self, swap: str, qty: int, success: bool):
        pass

    async def bot_handle_news(self, news_release: dict):

        # Parsing the message based on what type was received
        timestamp = news_release["timestamp"] # This is in exchange ticks not ISO or Epoch
        news_type = news_release['kind']
        news_data = news_release["new_data"]
        
        print("News Received")
        
        #for key in news_release:
           # print(key, news_release[key])
        
        
        if news_type == "structured":
            subtype = news_data["structured_subtype"]
            symb = news_data["asset"]
            if subtype == "earnings":
                
                earnings = news_data["value"]

                # Do something with this data

            else:

             
                new_signatures = news_data["new_signatures"]
                self.signatures.append((timestamp, new_signatures))
                print(self.signatures)
                cumulative = news_data["cumulative"]
                print(f"cum: {cumulative}")
            

                # Do something with this data
        else:
            self.uns_tstmps.append(timestamp)
            print(self.uns_tstmps)
            # Not sure what you would do with unstructured data....

            pass
    
    async def calc_akav_nav(self):
        stocks = {}
        for security, book in self.order_books.items():
                if book.bids.items() and book.asks.items():
                    highest_bid = max((k,v) for k,v in book.bids.items() if v != 0)
                    lowest_ask = min((k,v) for k,v in book.asks.items() if v != 0)
                    stocks[security] = (highest_bid+lowest_ask)/2

        if ('MKJ' in stocks) and ('APT' in stocks) and ('DLR' in stocks):
            total = 0
            equities = ["MKJ", "APT", "DLR"]
            for stock, nav in stocks:
                if stock in equities:
                    total += nav 
            return total
        return 0
    
    async def find_etf_arb(self):
        stocks = {}
        for security, book in self.order_books.items():
            try:
                highest_bid = max((px,qty) for px,qty in book.bids.items() if qty != 0)
                lowest_ask = min((px,qty) for px,qty in book.asks.items() if qty != 0)
                stocks[security] = [highest_bid, lowest_ask]
            except:
                print(f"No spread for {security}")
        if ("AKAV" in stocks) and ('MKJ' in stocks) and ('APT' in stocks) and ('DLR' in stocks):
            if stocks["AKAV"][0][0] > (stocks["MKJ"][1][0] + stocks["APT"][1][0] + stocks["DLR"][1][0] + 5): 
                qty = min(stocks["AKAV"][0][1], stocks["MKJ"][1][1], stocks["APT"][1][1], stocks["DLR"][1][1], max_size)         
                print(f'arb found, value = {qty*(stocks["AKAV"][0][0] - (stocks["MKJ"][1][0] + stocks["APT"][1][0] + stocks["DLR"][1][0]))-5}')
                print(f'AKAV BB = {stocks["AKAV"][0]}')
                print(f'MKJ BO = {stocks["MKJ"][1]}')
                print(f'APT BO = {stocks["APT"][1]}')
                print(f'DLR BO = {stocks["DLR"][1]}')

                #execute trades
                """
                await self.place_order("MKJ", qty, xchange_client.Side.BUY, stocks["MKJ"][1][0])
                await self.place_order("APT", qty, xchange_client.Side.BUY, stocks["APT"][1][0])
                await self.place_order("DLR", qty, xchange_client.Side.BUY, stocks["DLR"][1][0])
                await self.place_order("AKAV", qty, xchange_client.Side.SELL, stocks["AKAV"][0][0])
                await self.place_swap_order('toAKAV', qty)
                print("my positions:", self.positions)
                """
                await self.place_order("MKJ", qty, xchange_client.Side.BUY)
                await self.place_order("APT", qty, xchange_client.Side.BUY)
                await self.place_order("DLR", qty, xchange_client.Side.BUY)
                await self.place_order("AKAV", qty, xchange_client.Side.SELL)
                await self.place_swap_order('toAKAV', qty)
                print("my positions:", self.positions)
                
            if stocks["AKAV"][1][0] < (stocks["MKJ"][0][0] + stocks["APT"][0][0] + stocks["DLR"][0][0] - 5):
                qty = min(stocks["AKAV"][1][1], stocks["MKJ"][0][1], stocks["APT"][0][1], stocks["DLR"][0][1], max_size) 
                print(f'arb found, value = {qty*((stocks["MKJ"][0][0] + stocks["APT"][0][0] + stocks["DLR"][0][0]) - stocks["AKAV"][1][0])-5}')
                print(f'AKAV BO = {stocks["AKAV"][1]}')
                print(f'MKJ BB = {stocks["MKJ"][0]}')
                print(f'APT BB = {stocks["APT"][0]}')
                print(f'DLR BB = {stocks["DLR"][0]}')

                #execute trades
                """
                await self.place_order("MKJ", qty, xchange_client.Side.SELL, stocks["MKJ"][0][0])
                await self.place_order("APT", qty, xchange_client.Side.SELL, stocks["APT"][0][0])
                await self.place_order("DLR", qty, xchange_client.Side.SELL, stocks["DLR"][0][0])
                await self.place_order("AKAV", qty, xchange_client.Side.BUY, stocks["AKAV"][1][0])
                await self.place_swap_order('toAKAV', qty)
                print("my positions:", self.positions)
                """
                await self.place_order("MKJ", qty, xchange_client.Side.SELL)
                await self.place_order("APT", qty, xchange_client.Side.SELL)
                await self.place_order("DLR", qty, xchange_client.Side.SELL)
                await self.place_order("AKAV", qty, xchange_client.Side.BUY)
                await self.place_swap_order('fromAKAV', qty)
                print("my positions:", self.positions)
            else:
                print("Currently no arb")
        else:
            print("Currently no offers")
        #await self.liquidate_assets()

    async def liquidate_assets(self):
        for asset in self.positions:
            qty  = self.positions[asset]
            if asset != "cash":
                while qty != 0:
                    print("Selling Positions")
                    if qty > 0:
                        qty  = min(40, qty)
                        await self.place_order(asset, qty, xchange_client.Side.SELL)
                        qty = self.positions[asset] - qty
                    else:
                        qty = -qty
                        qty = min(40, qty)
                        await self.place_order(asset, qty, xchange_client.Side.BUY)
                        qty = self.positions[asset] + qty

    async def track_stock(self, ticker):
        while True:
            pass
        
            
                    


    async def trade(self):
        await asyncio.sleep(5)
        while True:
            await asyncio.sleep(1)
            print("attempting to trade")
            await self.find_etf_arb(30)
            await self.liquidate_assets()
            #await self.track_stock("MKJ")
        """
        await self.place_order("APT",3, xchange_client.Side.BUY, 5)
        await self.place_order("APT",3, xchange_client.Side.SELL, 7)
        await asyncio.sleep(5)
        await self.cancel_order(list(self.open_orders.keys())[0])
        await self.place_swap_order('toAKAV', 1)
        await asyncio.sleep(5)
        await self.place_swap_order('fromAKAV', 1)
        await asyncio.sleep(5)
        await self.place_order("APT",1000, xchange_client.Side.SELL, 7)
        await asyncio.sleep(5)
        market_order_id = await self.place_order("APT",10, xchange_client.Side.SELL)
        print("MARKET ORDER ID:", market_order_id)
        await asyncio.sleep(5)
        print("my positions:", self.positions)
        """

    async def view_books(self):
        while True:
            await asyncio.sleep(3)
            for security, book in self.order_books.items():
                sorted_bids = sorted((k,v) for k,v in book.bids.items() if v != 0)
                sorted_asks = sorted((k,v) for k,v in book.asks.items() if v != 0)
                print(f"Bids for {security}:\n{sorted_bids}")
                print(f"Asks for {security}:\n{sorted_asks}")

    async def start(self, user_interface):
        asyncio.create_task(self.trade())

        # This is where Phoenixhood will be launched if desired. There is no need to change these
        # lines, you can either remove the if or delete the whole thing depending on your purposes.
        if user_interface:
            self.launch_user_interface()
            asyncio.create_task(self.handle_queued_messages())

        await self.connect()


async def main(user_interface: bool):
    # SERVER = '127.0.0.1:8000'   # run locally
    SERVER = '3.138.154.148:3333' # run on sandbox
    TEAMNAME = "chicago3"
    PASSWORD = "8m@2tMpM$Q"
    my_client = MyXchangeClient(SERVER,TEAMNAME,PASSWORD)
    await my_client.start(user_interface)
    return

if __name__ == "__main__":

    # This parsing is unnecessary if you know whether you are using Phoenixhood.
    # It is included here so you can see how one might start the API.

    parser = argparse.ArgumentParser(
        description="Script that connects client to exchange, runs algorithmic trading logic, and optionally deploys Phoenixhood"
    )

    parser.add_argument("--phoenixhood", required=False, default=False, type=bool, help="Starts phoenixhood API if true")
    args = parser.parse_args()

    user_interface = args.phoenixhood

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(main(user_interface))



