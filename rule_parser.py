from pandas import DataFrame


class RuleParser:

    def __init__(self, data: DataFrame, init_table: DataFrame):
        self.data = data
        self.init_table = init_table
        self.start_date_dict = dict(zip(range(init_table.shape[0]), init_table.start_date.to_list()))
        self.finish_date_dict = dict(zip(range(init_table.shape[0]), init_table.finish_date.to_list()))
        self.wplace_type_dict = dict(zip(range(init_table.shape[0]), [int(x) for x in init_table.wplace_type.to_list()]))

    def transfrom(self, tab_num_index: int):
        wplace_type = self.wplace_type_dict[tab_num_index]
        if wplace_type == 0:
            print('wtype 0')
            return self.transformer_0(data=self.data, tab_num_index=tab_num_index)
        elif wplace_type == 1:
            print('wtype 1')
            return self.transformer_1(data=self.data, tab_num_index=tab_num_index)
        elif wplace_type == 2:
            print('wtype 2')
            return self.transformer_2(data=self.data, tab_num_index=tab_num_index)
        elif wplace_type == 3:
            print('wtype 3')
            return self.transformer_3(data=self.data, tab_num_index=tab_num_index)
        elif wplace_type == 4:
            print('wtype 4')
            return self.transformer_4(data=self.data, tab_num_index=tab_num_index)

    def transformer_0(self, data: DataFrame, tab_num_index: int):
        data.loc[
            (data['tab_num'] == self.init_table.tab_num.to_list()[tab_num_index]) &
            (data['ymd_date'] <= self.finish_date_dict[tab_num_index].to_datetime64()) &
            (data['ymd_date'] >= self.start_date_dict[tab_num_index].to_datetime64()) &
            (data['weekday'] != 5) &
            (data['weekday'] != 6),
            'to_be_at_office'
        ] = 1
        self.data = data
        return data

    def transformer_1(self, data: DataFrame, tab_num_index: int):
        data.loc[
            (data['tab_num'] == self.init_table.tab_num.to_list()[tab_num_index]) &
            (data['ymd_date'] <= self.finish_date_dict[tab_num_index].to_datetime64()) &
            (data['ymd_date'] >= self.start_date_dict[tab_num_index].to_datetime64()) &
            (data['weekday'] != 5) &
            (data['weekday'] != 6),
            'to_be_at_office'
        ] = 0
        self.data = data
        return data

    def transformer_2(self, data: DataFrame, tab_num_index: int):
        data.loc[
            (
                    (data['tab_num'] == self.init_table.tab_num.to_list()[tab_num_index]) &
                    (data['ymd_date'] < self.finish_date_dict[tab_num_index].to_datetime64()) &
                    (data['ymd_date'] > self.start_date_dict[tab_num_index].to_datetime64()) &
                    (data['weekday'] != 5) &
                    (data['weekday'] != 6)
            ) &
                (data['weekday'].isin([1, 2, 3, 4])),
            'to_be_at_office'
        ] = 1
        self.data = data
        return data

    def transformer_3(self, data: DataFrame, tab_num_index: int):
        data.loc[
            (
                    (data['tab_num'] == self.init_table.tab_num.to_list()[tab_num_index]) &
                    (data['ymd_date'] <= self.finish_date_dict[tab_num_index].to_datetime64()) &
                    (data['ymd_date'] >= self.start_date_dict[tab_num_index].to_datetime64()) &
                    (data['weekday'] != 5) &
                    (data['weekday'] != 6)
            )
            &
                data['weekday'].isin([0, 1, 2, 3, 4])
            &
                data['week'] % 2 == 0,
            'to_be_at_office'
        ] = 0
        data.loc[
            (
                    (data['tab_num'] == self.init_table.tab_num.to_list()[tab_num_index]) &
                    (data['ymd_date'] <= self.finish_date_dict[tab_num_index].to_datetime64()) &
                    (data['ymd_date'] >= self.start_date_dict[tab_num_index].to_datetime64()) &
                    (data['weekday'] != 5) &
                    (data['weekday'] != 6)
            )
            &
                data['weekday'].isin([0, 1, 2, 3, 4])
            &
                data['week'] % 2 == 1,
            'to_be_at_office'
        ] = 1
        self.data = data
        return data

    def transformer_4(self, data: DataFrame, tab_num_index: int):
        data.loc[
            (
                    (data['tab_num'] == self.init_table.tab_num.to_list()[tab_num_index]) &
                    (data['ymd_date'] < self.finish_date_dict[tab_num_index].to_datetime64()) &
                    (data['ymd_date'] > self.start_date_dict[tab_num_index].to_datetime64()) &
                    (data['weekday'] != 5) &
                    (data['weekday'] != 6)
            ) &
                (data['weekday'].isin([1, 2, 3, 4])),
            'to_be_at_office'
        ] = 1
        self.data = data
        return data