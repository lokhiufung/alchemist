from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from alchemist.gateways.ibkr.gateway import BOT, IbkrGateway, IbTransport


def create_transport(gateway: IbkrGateway) -> IbTransport:
    from ibapi.client import EClient
    from ibapi.common import BarData
    from ibapi.contract import Contract
    from ibapi.execution import ExecutionFilter
    from ibapi.order import Order
    from ibapi.wrapper import EWrapper

    def to_contract(spec: dict) -> Contract:
        contract = Contract()
        contract.secType = "FUT" if spec["sec_type"] == "future" else "STK"
        contract.symbol = spec["trading_class"] if spec["sec_type"] != "future" else spec.get("symbol", spec["trading_class"])
        contract.tradingClass = spec["trading_class"]
        contract.exchange = spec["venue"]
        contract.currency = spec["currency"]
        if spec.get("contract_month"):
            contract.lastTradeDateOrContractMonth = spec["contract_month"]
        return contract

    def spec_of(contract: Contract) -> dict:
        sec_type = "future" if contract.secType == "FUT" else "stock"
        return {"sec_type": sec_type, "trading_class": contract.tradingClass or contract.symbol,
                "venue": contract.exchange if sec_type == "future" else "SMART", "currency": contract.currency}

    class Client(EWrapper, EClient, IbTransport):
        def __init__(self) -> None:
            EWrapper.__init__(self)
            EClient.__init__(self, self)
            self.reader: threading.Thread | None = None

        def connect(self, host: str, port: int, client_id: int) -> None:
            EClient.connect(self, host, port, client_id)
            self.reader = threading.Thread(target=self.run, daemon=True, name="ibkr-reader")
            self.reader.start()

        def disconnect(self) -> None:
            EClient.disconnect(self)

        def nextValidId(self, orderId: int) -> None:
            gateway.on_next_valid_id(orderId)

        def currentTime(self, time_: int) -> None:
            gateway.on_current_time(float(time_))

        def req_current_time(self) -> None:
            self.reqCurrentTime()

        def error(self, reqId, errorCode, errorString, *args) -> None:
            gateway.on_error(reqId, errorCode, errorString)

        def req_contract_details(self, req_id: int, spec: dict) -> None:
            self.reqContractDetails(req_id, to_contract(spec))

        def contractDetails(self, reqId, details) -> None:
            gateway.on_contract_details(reqId, {"con_id": details.contract.conId, "min_tick": details.minTick})

        def contractDetailsEnd(self, reqId) -> None:
            gateway.on_contract_details_end(reqId)

        def req_tick_by_tick(self, req_id: int, contract: dict) -> None:
            self.reqTickByTickData(req_id, to_contract(contract), "AllLast", 0, False)

        def cancel_tick_by_tick(self, req_id: int) -> None:
            self.cancelTickByTickData(req_id)

        def tickByTickAllLast(self, reqId, tickType, time_, price, size, attribs, exchange, specialConditions) -> None:
            gateway.on_tick(gateway.generation, reqId, float(time_), price, float(size))

        def req_order_updates(self) -> None:
            self.reqAutoOpenOrders(True)

        def req_account_updates(self, accounts: list[str]) -> None:
            for account in accounts:
                self.reqAccountUpdates(True, account)

        def updateAccountValue(self, key, val, currency, accountName) -> None:
            gateway.on_account_value(accountName, currency, key, val)

        def place_order(self, order_id: int, contract: dict, spec: dict) -> None:
            order = Order()
            order.action, order.totalQuantity, order.orderType = spec["action"], spec["qty"], spec["order_type"]
            if spec["order_type"] == "LMT":
                order.lmtPrice = spec["limit_price"]
            if spec["order_type"] == "STP":
                order.auxPrice = spec["stop_price"]
            order.tif, order.outsideRth, order.account = spec["tif"], spec["outside_rth"], spec["account"]
            if spec["oca_group"]:
                order.ocaGroup, order.ocaType = spec["oca_group"], spec["oca_type"]
            self.placeOrder(order_id, to_contract(contract), order)

        def cancel_order(self, order_id: int) -> None:
            self.cancelOrder(order_id, "")

        def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice,
                        clientId, whyHeld, mktCapPrice=0.0) -> None:
            gateway.on_order_status(orderId, status, int(filled), int(remaining), avgFillPrice, lastFillPrice)

        def req_executions(self, req_id: int, since: float | None) -> None:
            self.reqExecutions(req_id, ExecutionFilter())

        def execDetails(self, reqId, contract, execution) -> None:
            moment = datetime.strptime(execution.time.split(" ")[0] + " " + execution.time.split(" ")[1], "%Y%m%d %H:%M:%S")
            gateway.on_exec_details(reqId, {**spec_of(contract), "order_id": execution.orderId,
                                            "exec_id": execution.execId, "ts": moment.replace(tzinfo=timezone.utc).timestamp(),
                                            "side": execution.side, "qty": int(execution.shares),
                                            "price": execution.price, "account": execution.acctNumber,
                                            "currency": contract.currency})

        def execDetailsEnd(self, reqId) -> None:
            gateway.on_exec_details_end(reqId)

        def req_open_orders(self) -> None:
            self.reqAllOpenOrders()

        def openOrder(self, orderId, contract, order, orderState) -> None:
            gateway.on_open_order({**spec_of(contract), "order_id": orderId, "side": order.action,
                                   "qty": int(order.totalQuantity), "order_type": order.orderType,
                                   "limit_price": order.lmtPrice, "stop_price": order.auxPrice,
                                   "oca_group": order.ocaGroup or None, "account_id": order.account})

        def openOrderEnd(self) -> None:
            gateway.on_open_order_end()

        def req_positions(self) -> None:
            self.reqPositions()

        def position(self, account, contract, position, avgCost) -> None:
            gateway.on_position({**spec_of(contract), "account": account, "size": position, "avg_cost": avgCost})

        def positionEnd(self) -> None:
            gateway.on_position_end()

        def req_historical_data(self, req_id: int, contract: dict, end: datetime, duration: str, bar_size: str) -> None:
            self.reqHistoricalData(req_id, to_contract(contract), end.strftime("%Y%m%d-%H:%M:%S"), duration,
                                   bar_size, "TRADES", 0, 2, False, [])

        def historicalData(self, reqId, bar: BarData) -> None:
            moment = datetime.strptime(bar.date, "%Y%m%d %H:%M:%S") if " " in bar.date else datetime.fromtimestamp(int(bar.date))
            gateway.on_historical_bar(reqId, {"start_ts": moment.replace(tzinfo=timezone.utc).timestamp(),
                                              "open": bar.open, "high": bar.high, "low": bar.low,
                                              "close": bar.close, "volume": float(bar.volume)})

        def historicalDataEnd(self, reqId, start, end) -> None:
            gateway.on_historical_end(reqId)

    return Client()
