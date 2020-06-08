from braintrust_defi.engine import get_engine

engine = get_engine();

print(engine.dydx.client.get_orderbook('PBTC-USDC'))
