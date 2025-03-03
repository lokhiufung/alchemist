from alchemist.convertor import Convertor


class IbConvertor(Convertor):

    ORDER_SIDE_MAPPING = {
        1: 'BUY',
        -1: 'SELL'
    }
    ORDER_TYPE_MAPPING = {
        'LIMIT': 'LMT',
        'MARKET': 'MKT'
    }
    TIME_IN_FORCE_MAPPING = {
        'GTC': 'GTC',
        'DAY': 'DAY',
    }

    def __init__(self):
        self.order_side_mapping = self.ORDER_SIDE_MAPPING
        self.order_type_mapping = self.ORDER_TYPE_MAPPING
        self.time_in_force_mapping = self.TIME_IN_FORCE_MAPPING
        self.inverse_order_side_mapping = self._get_inverse_mapping(self.order_side_mapping)
        self.inverse_order_type_mapping = self._get_inverse_mapping(self.order_type_mapping)
        self.inverse_time_in_force_mapping = self._get_inverse_mapping(self.time_in_force_mapping)

    @staticmethod
    def _get_inverse_mapping(mapping: dict):
        return {v: k for k, v in mapping.items()}
    
    def convert_order_side_to_external(self, order_status):
        return self.order_side_mapping[order_status]

    def convert_order_side_to_internal(self, order_status):
        return self.inverse_order_side_mapping[order_status]

    def convert_order_type_to_external(self, order_type):
        return self.order_type_mapping[order_type]
    
    def convert_order_type_to_internal(self, order_type):
        return self.inverse_order_type_mapping[order_type]
    
    def convert_time_in_force_to_external(self, time_in_force):
        return self.time_in_force_mapping[time_in_force]
    
    def convert_time_in_force_to_internal(self, time_in_force):
        return self.inverse_time_in_force_mapping[time_in_force]
    
    

