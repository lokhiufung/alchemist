from datetime import datetime

import pandas as pd

from alchemist.strategies.base_strategy import BaseStrategy
from alchemist.data_pipelines.base_data_pipeline import BaseDataPipeline
from alchemist.managers.backtest_manager import BacktestManager
from alchemist.datas.frequency import Frequency


class Backtester:

    BACKTEST_GATEWAY = 'ib_gateway'

    def __init__(self):
        self.strategy: BaseStrategy = None

        self.warmup_date: str = None
        self.base_freq: Frequency = None
        self.resample_freqs: list[str] = []
        self.updates = []
        self.signals = []
    
    def _sanity_check(self):
        pass

    def setup_warmup_date(self, warmup_date: str):
        self.warmup_date = warmup_date

    def setup_strategy(self, strategy: BaseStrategy, backtest_manager, initial_cash: float):
        self.strategy = strategy
        self.strategy.backtest_manager = backtest_manager
        # 1. setup initial captial
        self.strategy.update_balance('USD', initial_cash)
    
    def setup_historical_data(self, data_pipeline: BaseDataPipeline, start_date: str, end_date: str):
        data_cards = self.strategy.data_cards
        product = self.strategy.product
        # each data card corresponds to 1 bar data
        data_cards_sorted = sorted(data_cards, key=lambda data_card: data_card.frequency)
        
        self.base_freq = data_cards_sorted[0].frequency.freq
        resample_freqs = [frequency.freq for frequency in data_cards_sorted[1:]]

        self.updates = data_pipeline.historical_bars(
            product=product,
            freq=self.base_freq,
            start_date=start_date,
            end_date=end_date,
            auto_resample_freqs=resample_freqs,
        )

    def check_warmup(self, ts):
        if self.warmup_date is None:
            return False
        warmup_dt = datetime.strptime(self.warmup_date, '%Y-%m-%d')
        return ts < warmup_dt
    
    def export_backtest_data(self, path_prefix: str):
        signals = pd.DataFrame(self.signals)
        signals.set_index('ts', inplace=True)
        df_portfolio = pd.DataFrame(self.strategy.backtest_manager.portfolio_history)
        df_transactions = pd.DataFrame(self.strategy.backtest_manager.transaction_log)

        df_portfolio.to_csv(f'{path_prefix}_portfolio.csv', index=False)
        df_transactions.to_csv(f'{path_prefix}_transactions.csv', index=False)
        signals.to_csv(f'{path_prefix}_signals.csv')

    def start_backtesting(
        self,
        strategy: BaseStrategy,
        data_pipeline: BaseDataPipeline,
        start_date: str,
        end_date: str,
        initial_cash: float,
        commission=None,
        warmup_date: str = None,  # before this date is warmup period
        export_data=False,
        path_prefix=''
    ):
        
        backtest_manager = BacktestManager(strategy, strategy.pm, commission=commission)

        # setup strategy initial status
        self.setup_strategy(
            strategy,
            backtest_manager,
            initial_cash
        )

        # setup data
        self.setup_historical_data(data_pipeline, start_date, end_date)
        
        # setup warmup date
        self.setup_warmup_date(warmup_date)

        # main loop
        for update in self.updates:

            update['ts'] = datetime.fromtimestamp(update['ts'])
            
            is_warmup = self.check_warmup(ts=update['ts'])

            self.strategy._on_bar(
                gateway=self.BACKTEST_GATEWAY,
                exch=self.strategy.product.exch,
                pdt=self.strategy.product.name,
                freq=update['resolution'],
                ts=update['ts'],
                open_=update['data']['open'],
                high=update['data']['high'],
                low=update['data']['low'],
                close=update['data']['close'],
                volume=update['data']['volume'],
                is_warmup=is_warmup,  # only advance the indicators during warmup period
            )

            if not is_warmup:
                self.signals.append({'ts': update['ts'], **self.strategy.get_signals()})
            
            if update['resolution'] == self.base_freq:
                backtest_manager.on_bar(
                    gateway=self.BACKTEST_GATEWAY,
                    exch=self.strategy.product.exch,
                    pdt=self.strategy.product.name,
                    freq=self.base_freq,
                    ts=update['ts'],
                    open_=update['data']['open'],
                    high=update['data']['high'],
                    low=update['data']['low'],
                    close=update['data']['close'],
                    volume=update['data']['volume'],
                    on_order_status_update=self.strategy.on_order_status_update
                )

        if export_data:
            self.export_backtest_data(
                path_prefix=path_prefix
            )            

        

            


            
        

