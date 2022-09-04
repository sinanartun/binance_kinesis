import asyncio
import boto3
from binance import AsyncClient, BinanceSocketManager
import random
import datetime
import json
import os

data_stream_name = os.getenv('data_stream_name')

kinesis = boto3.client('kinesis')


async def main():
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    ts = bm.trade_socket('BTCUSDT')
    async with ts as tscm:
        while True:
            res = await tscm.recv()
            timestamp = f"{datetime.datetime.fromtimestamp(int(res['T'] / 1000)):%Y-%m-%d %H:%M:%S}"
            maker = '0'
            if res['m']:  # Satın almış ise 1, satış yaptı ise 0.
                maker = '1'
            partition_key = random.randint(10, 100)

            dic = {
                "t": str(res['t']),
                "s": str(res['s']),
                "p": '{:.2f}'.format(round(float(res['p']), 2)),
                "q": str(res['q'])[0:-3],
                "ts": str(timestamp),
                "m": int(maker),
            }
            response = kinesis.put_record(StreamName=data_stream_name, Data=json.dumps(dic), PartitionKey=str(partition_key))

    await client.close_connection()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
