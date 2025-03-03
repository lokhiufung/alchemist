from alchemist.datas.base_data import BaseData
from alchemist.datas.common import Tick


class TickData(BaseData):

    def on_tick_update(self, ts, price, size):
        self.total_len += 1
        self.data.append(Tick(ts, price, size))
        self._step()
        self.push()


