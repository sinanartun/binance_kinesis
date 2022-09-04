import asyncio
import boto3
from binance import AsyncClient, BinanceSocketManager
import random
import datetime
import json

kinesis = boto3.client('kinesis')


async def main():
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    # start any sockets here, i.e a trade socket
    ts = bm.trade_socket('BTCUSDT')
    # then start receiving messages
    async with ts as tscm:
        while True:
            res = await tscm.recv()
            timestamp = f"{datetime.datetime.fromtimestamp(int(res['T'] / 1000)):%Y-%m-%d %H:%M:%S}"
            maker = '0'
            if res['m']:  # Satın almış ise 1, satış yaptı ise 0.
                maker = '1'
            partitionkey = random.randint(10, 100)

            dic = {
                "t": str(res['t']),
                "s": str(res['s']),
                "p": '{:.2f}'.format(round(float(res['p']), 2)),
                "q": str(res['q'])[0:-3],
                "ts": str(timestamp),
                "m": int(maker),
            }
            print(json.dumps(dic, sort_keys=False, default=str))
            # line = str(res['t']) + '\t'
            # line += str(res['s']) + '\t'
            # line += '{:.2f}'.format(round(float(res['p']), 2)) + '\t'
            # line += str(res['q'])[0:-3] + '\t'
            # line += str(timestamp) + '\t'
            # line += str(maker)
            # mydata = '{ "vibration": ' + str(v) + ', "temperature": ' + str(t) + ', "pressure": ' + str(p) + '}'
            response = kinesis.put_record(StreamName='binancedatastream', Data=json.dumps(dic), PartitionKey=str(partitionkey))
            print(response)

    await client.close_connection()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())