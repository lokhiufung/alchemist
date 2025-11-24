from alchemist.datas.base_data import BaseData
from alchemist.datas.common import Tick


class TickData(BaseData):
    DATA_LINES = (
        "ts",
        "price",
        "size",
    )

    def on_tick_update(self, ts, price, size):
        self._step()

        self.price.append(price)
        self.size.append(size)
        self.ts.append(ts)

        # self.push()

    def __getitem__(self, index):
        if isinstance(index, slice):
            ts = self.ts[index]
            price = self.price[index]
            size = self.size[index]
            return [
                Tick(t, p, s) 
                for t, p, s in zip(ts, price, size)
            ]
        return Tick(
            self.ts[index],
            self.price[index],
            self.size[index]
        )


