# Write your code here
import pandas as pd
import json
import string
import sqlite3


def strip_suffix(file_name):
    i = file_name.index('.')
    return file_name[:i], file_name[i:]


def plural_single(n):
    if n == 1:
        return ' was'
    else:
        return 's were'


def score_capacity(x):
    if x < 20:
        return 0
    else:
        return 2


def score_fuel(x):
    if x <= 230:
        return 2
    else:
        return 1


def score_pitstops(x):
    if x < 1:
        return 2
    elif x < 2:
        return 1
    else:
        return 0


def score(df_in):
    route_length = 4.5
    pitstops = route_length * df_in.fuel_consumption / df_in.engine_capacity
    fuel_burned = route_length * df_in.fuel_consumption
    total = pitstops.apply(score_pitstops)\
        + fuel_burned.apply(score_fuel) \
        + df_in.maximum_load.apply(score_capacity)
    total.name = 'score'
    #print('----')
    #print(total)
    return total


class Spreadsheet:
    def __init__(self):
        self.corrected_cells = 0
        self.DB_exist = False
        self.is_checked = False
        self.CSV_exist = False
        self.JSON_exist = False
        self.file_name_root, self.suffix_in = strip_suffix(input('Input file name\n'))
        if self.suffix_in == '.s3db':
            self.CSV_exist = True
            self.is_checked = True
            self.DB_exist = True
        elif '[CHECKED]' in self.file_name_root:
            self.file_name_root = self.file_name_root.rstrip('[CHECKED]')
            self.CSV_exist = True
            self.is_checked = True
        elif self.suffix_in == '.csv':
            self.CSV_exist = True
        self.suffix_out = '.csv'
        # Get database connection
        self.conn = sqlite3.connect(f'{self.file_name_root}.s3db')
        self.cursor = self.conn.cursor()

    def read_file(self):
        if self.suffix_in == '.xlsx':
            df_in = pd.read_excel(self.file_name_root + self.suffix_in, sheet_name='Vehicles')
        elif self.suffix_in == '.csv' and self.is_checked:
            df_in = pd.read_csv(self.file_name_root + '[CHECKED]' + self.suffix_in)
        elif self.suffix_in == '.csv':
            df_in = pd.read_csv(self.file_name_root + self.suffix_in)
        elif self.suffix_in == '.s3db':
            df_in = pd.read_sql('select * from convoy', self.conn)
        return df_in

    def process_data(self):
        df_in = self.read_file()
        #print(df_in)
        if not self.CSV_exist:
            self.write_csv_file(df_in)
            df_out = self.check_dataframe(df_in)
            self.write_csv_file(df_out)
            self.separate_dataframe(df_out)
        elif not self.is_checked:
            df_out = self.check_dataframe(df_in)
            self.write_csv_file(df_out)
            self.separate_dataframe(df_out)
        elif not self.DB_exist:
            self.separate_dataframe(df_in)
        elif not self.JSON_exist:
            self.separate_dataframe(df_in)

    def separate_dataframe(self, df_in):
        scored_col = score(df_in)
        #print(scored_col)
        df_in['score'] = scored_col
        #print(df_in)
        df_db = df_in
        df_json = df_db[df_db['score'] > 3]
        df_json = df_json.drop(columns=['score'])
        df_xml = df_db[df_db['score'] <= 3]
        df_xml = df_xml.drop(columns=['score'])
        if not self.DB_exist:
            self.insert_in_db(df_db)
            self.write_json(df_json)
            self.write_xml(df_xml)
        if not self.JSON_exist:
            self.write_json(df_json)
            self.write_xml(df_xml)

    def write_json(self, df_out):
        suffix = '.json'
        filename = self.file_name_root + suffix
        # json_string = build_table_schema(df_out)
        n_vehicles = df_out.shape[0]
        dict_out = {'convoy': json.loads(df_out.to_json(orient='records'))}
        with open(filename, 'w') as f_out:
            json.dump(dict_out, f_out)
        print(f'{n_vehicles} vehicle{plural_single(n_vehicles)} saved into {filename}')

    def write_xml(self, df_out):
        suffix = '.xml'
        filename = self.file_name_root + suffix
        n_vehicles = df_out.shape[0]
        if n_vehicles == 0:
            xml_out = '<convoy></convoy>'
        else:
            xml_out = df_out.to_xml(root_name='convoy', row_name='vehicle', index=False, xml_declaration=False)
        with open(filename, 'w') as f_out:
            f_out.write(xml_out)
        print(f'{n_vehicles} vehicle{plural_single(n_vehicles)} saved into {filename}')
        print(xml_out)

    def write_csv_file(self, df_out):
        suffix = '.csv'
        file_name = self.file_name_root + suffix
        if self.is_checked:
            file_name = self.file_name_root + '[CHECKED]' + suffix
        df_out.to_csv(file_name, index=False)
        n_lines = df_out.shape[0]
        if not self.is_checked:
            print(f'{n_lines} line{plural_single(n_lines)} added to {file_name}')
        else:
            print(f'{self.corrected_cells} cell{plural_single(self.corrected_cells)} corrected in {file_name}')

    def select_numbers(self, line):
        if type(line) == str and not line.isdecimal():
            self.corrected_cells += 1
            new_line = ''
            for a in line:
                if a in string.digits:
                    new_line += a
            return int(new_line)
        else:
            return int(line)

    def check_dataframe(self, df_in):
        for col in df_in.columns:
            if df_in[col].dtype == 'object':
                for i, item in enumerate(df_in[col]):
                    df_in[col][i] = self.select_numbers(item)
        self.is_checked = True
        return df_in

    def insert_in_db(self, df_out):
        db_suffix = '.s3db'
        col_names = {'vehicle_id': 'INTEGER PRIMARY KEY',
                     'engine_capacity': 'INTEGER NOT NULL',
                     'fuel_consumption': 'INTEGER NOT NULL',
                     'maximum_load': 'INTEGER NOT NULL',
                     'score': 'INTEGER NOT NULL'}
        df_out.to_sql('convoy', self.conn, index=False, if_exists='replace', dtype=col_names)
        self.conn.commit()
        self.conn.close()
        n_records = df_out.shape[0]
        print(f'{n_records} record{plural_single(n_records)} inserted into {self.file_name_root + db_suffix}')


if __name__ == '__main__':
    s = Spreadsheet()
    s.process_data()
