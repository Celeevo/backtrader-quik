"""Read-only by default live example for backtrader-quik.

Edit ACCOUNT_ID and DATA_NAME. Set ENABLE_TRADING=True only after validating
bars, account mapping, lot size and order lifecycle in QUIK Junior.
"""
from __future__ import annotations

from datetime import datetime

import backtrader as bt

from backtrader_quik import (
    QKStore,
    configure_console_logging,
    logger,
    set_file_logging,
)


ACCOUNT_ID = 0
DATA_NAME = 'SPBFUT.RIU6'  # Replace with an actually available instrument
ENABLE_TRADING = False
ORDER_SIZE = 1             # Contracts for futures; shares and lot-multiple for stocks
HOLD_BARS = 2
COMMISSION = 0.0005        # Example estimate: 0.05%; adapt to your broker
WRITE_FILE_LOG = False


class SafeLiveStrategy(bt.Strategy):
    params = dict(
        enable_trading=ENABLE_TRADING,
        order_size=ORDER_SIZE,
        hold_bars=HOLD_BARS,
    )

    def __init__(self):
        self.is_live = False
        self.pending_order = None
        self.entry_bar = None

    def notify_data(self, data, status, *args, **kwargs):
        status_name = data._getstatusname(status)
        logger.info('Data %s status: %s', data._name, status_name)
        self.is_live = status == data.LIVE

    def notify_order(self, order):
        logger.info(
            'Order ref=%s tradeid=%s status=%s executed=%s remaining=%s',
            order.ref,
            order.tradeid,
            order.getstatusname(),
            order.executed.size,
            order.executed.remsize,
        )

        if order.status in (order.Submitted, order.Accepted, order.Partial):
            return

        if order.status == order.Completed:
            logger.info(
                'Execution %s size=%s price=%s value=%s commission=%s',
                'BUY' if order.isbuy() else 'SELL',
                order.executed.size,
                order.executed.price,
                order.executed.value,
                order.executed.comm,
            )
            if order.isbuy() and self.position.size:
                self.entry_bar = len(self)
            elif order.issell() and not self.position.size:
                self.entry_bar = None
        else:
            logger.error(
                'Order finished without completion: %s info=%s',
                order.getstatusname(),
                dict(order.info),
            )

        if self.pending_order is not None and order.ref == self.pending_order.ref:
            self.pending_order = None

    def notify_trade(self, trade):
        if trade.isclosed:
            logger.info(
                'Trade closed: tradeid=%s pnl=%s pnlcomm=%s commission=%s',
                trade.tradeid,
                trade.pnl,
                trade.pnlcomm,
                trade.commission,
            )

    def next(self):
        logger.info(
            'Bar dt=%s O=%s H=%s L=%s C=%s V=%s position=%s cash=%s value=%s',
            bt.num2date(self.data.datetime[0]),
            self.data.open[0],
            self.data.high[0],
            self.data.low[0],
            self.data.close[0],
            self.data.volume[0],
            self.position.size,
            self.broker.getcash(),
            self.broker.getvalue(),
        )

        if not self.is_live:
            return
        if not self.p.enable_trading:
            logger.info('Read-only mode: ENABLE_TRADING=False')
            return
        if self.pending_order is not None:
            return
        if len(self.data) < 2:
            return

        if not self.position:
            two_bullish = (
                self.data.close[0] > self.data.open[0]
                and self.data.close[-1] > self.data.open[-1]
            )
            if two_bullish:
                self.pending_order = self.buy(
                    size=self.p.order_size,
                    tradeid=1,
                )
                logger.info('BUY sent, ref=%s', self.pending_order.ref)
        elif self.entry_bar is not None and len(self) >= self.entry_bar + self.p.hold_bars:
            self.pending_order = self.close(tradeid=1)
            logger.info('CLOSE sent, ref=%s', self.pending_order.ref)


def main():
    configure_console_logging()
    if WRITE_FILE_LOG:
        path = set_file_logging(True)
        logger.info('File log: %s', path)

    store = QKStore()
    try:
        logger.info('Available accounts:')
        for account in store.provider.accounts:
            logger.info('%s', account)

        broker = store.getbroker(account_id=ACCOUNT_ID)
        symbol_info = broker.check_data_names(DATA_NAME)
        logger.info('Symbol info: %s', symbol_info)

        if not DATA_NAME.startswith('SPBFUT.'):
            lot_size = int(symbol_info['lot_size'])
            if ORDER_SIZE % lot_size:
                raise ValueError(
                    f'ORDER_SIZE={ORDER_SIZE} must be a multiple of lot_size={lot_size}'
                )

        cerebro = bt.Cerebro(stdstats=False, quicknotify=True)
        cerebro.setbroker(broker)
        broker.setcommission(commission=COMMISSION)

        data = store.getdata(
            dataname=DATA_NAME,
            timeframe=bt.TimeFrame.Minutes,
            compression=1,
            fromdate=datetime.now().date(),
            live_bars=True,
        )
        cerebro.adddata(data, name=DATA_NAME)
        cerebro.addstrategy(SafeLiveStrategy)
        cerebro.run()
    finally:
        # Idempotent: safe even if Cerebro has already stopped the store.
        store.stop()


if __name__ == '__main__':
    main()
