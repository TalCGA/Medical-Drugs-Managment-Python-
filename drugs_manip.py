import dataclasses
import csv
import re
from typing import Optional, Tuple, Dict

# global strings
csv_file_name = 'Products_db.csv'


@dataclasses.dataclass(frozen=True)
class Drug:
    appl_no: int
    product_no: int
    form: Tuple[str, ...]
    strength: Tuple[str, ...]
    reference_drug: int
    drug_name: Tuple[str, ...]
    active_ingredients: Tuple[str, ...]
    reference_standard: Optional[int]

    # gets the vals of drug as a string
    def get_drug_string(self):
        new_line = ""
        for value in vars(self).values():
            new_line += str(value) + '\t'
        # remove the redundant \t
        new_line = new_line.removesuffix('\t')
        # add new line for next insert
        return new_line


# -static functions- in order to keep the init function elegant and clean
def load_txt_file_to_csv_file(txt_file):
    with open(txt_file) as infile, open(csv_file_name, 'w', encoding='UTF8') as outfile:
        writer = csv.writer(outfile)
        # skip the headers line
        next(infile)
        for row in infile:
            writer.writerow(x.strip() for x in row.split('\t'))


def create_db(db_table):
    db = []
    for row in db_table:
        if isinstance(row[0], int) or isinstance(row[1], int):
            continue
        drug = Drug(int(row[0]), int(row[1]), re.split('; |;', row[2]),
                    re.split('; |;', row[3]), row[4], re.split('; |;', row[5]),
                    re.split('; |;', row[6]), row[7])
        db.append(drug)
    return db


@dataclasses.dataclass
class DrugDB:
    drugs: list[Drug]
    load_txt_file: str

    def __init__(self, load_txt_file):
        self.load_txt_file = load_txt_file
        load_txt_file_to_csv_file(load_txt_file)
        with open(csv_file_name, 'r', encoding='UTF8') as f:
            reader = csv.reader(f)
            self.drugs = create_db(reader)

    def query(self, key, value):
        res = Query(self.getdb_callback, key, value)
        return res

    def getdb_callback(self):
        return self.drugs

    # I suppose that the primary key is appl_no and product_no, but I wrote as a dict (open to change)
    def is_unique(self, primary_key):
        for drug in self.drugs:
            if drug.appl_no == list(primary_key.values())[0] and drug.product_no == list(primary_key.values())[1]:
                return False
        return True

    def get_drug_index_by_primary_key(self, primary_key):
        index = 1
        for drug in self.drugs:
            if list(primary_key.values())[0] == getattr(drug, list(primary_key.keys())[0]):
                if list(primary_key.values())[1] == getattr(drug, list(primary_key.keys())[1]):
                    return index
            index += 1
        # case no drug was found
        return -1

    def append_new_drug(self, new_drug):
        self.drugs.append(new_drug)

    def insert(self, new_drug):
        # Checks if the primary key is valid before insert
        if not self.is_unique({"appl_no": new_drug.appl_no, "product_no": new_drug.product_no}):
            print("Cannot insert new drug, please provide unique appl_no and product_no!\n")
            return False
        str_drug = new_drug.get_drug_string()
        with open(self.load_txt_file, 'a', encoding='UTF8') as f:
            f.write(str_drug)
        self.append_new_drug(new_drug)
        print("A new drug {} has been successfully ADDED to db and txt file!\n".format(new_drug.drug_name))

    def delete(self, del_drug):
        i = 0
        index_del_drug = self.get_drug_index_by_primary_key({"appl_no": del_drug.appl_no,
                                                             "product_no": del_drug.product_no})
        if index_del_drug == -1:
            print("Cannot delete drug {} because it doesn't exists in this Drugdb!\n".format(del_drug.drug_name))
            return False
        with open(self.load_txt_file, "r") as f:
            lines = f.readlines()
        with open(self.load_txt_file, "w") as f:
            for line in lines:
                # better performance than searching in txt file- the txt file and db are synchronized
                if i != index_del_drug:
                    f.write(line)
                i += 1
        self.drugs.pop(index_del_drug - 1)
        print("The drug {} has been successfully DELETED in db and txt file!\n".format(del_drug.drug_name))

    def print_drugs(self):
        print(*self.drugs, sep="\n")


class Query:

    def __init__(self, getdb_callback, init_key, init_value):
        self.changes = {init_key: init_value}
        self.getdb_callback = getdb_callback

    def filter(self, key, value):
        self.changes[key] = value
        return self

    def results(self):
        filtered_data = []
        is_query = True
        is_tuple = False
        for k, v in self.changes.items():
            if is_query:
                drugs = self.getdb_callback()
            else:
                drugs = filtered_data
            for drug in reversed(drugs):
                # check is type is int or tuple because the next 'if' is different on each state
                if k.lower() in ['appl_no', 'product_no', 'reference_drug', 'reference_standard']:
                    is_tuple = True
                attr_as_list = (getattr(drug, k.lower()), [getattr(drug, k.lower())])[is_tuple]

                if v in attr_as_list:
                    if is_query:
                        filtered_data.append(drug)
                else:
                    if not is_query:
                        filtered_data.remove(drug)
            is_query = False

        print("Retrieve", len(filtered_data), "records")
        if len(filtered_data) > 0:
            print(*filtered_data, sep="\n")
        print()
        return filtered_data


if __name__ == '__main__':
    # retrieve 92 records
    trimethoprim_drugs = drug_db.query('active_ingredients', 'TRIMETHOPRIM').results()
    # retrieve 2 records
    specific_trimethoprim_drugs = drug_db.query('active_ingredients', 'TRIMETHOPRIM').filter('appl_no', 17376).results()
    # retrieve 12 records
    acetaminophen_650mg = drug_db.query('drug_name', 'ACETAMINOPHEN').filter('strength', '650MG').results()
    
    drug_db = DrugDB('Products.txt')
    # retrieve 2 records
    drug_db.query("appl_no", 20812).results()
    # print INSERT ERROR
    drug_db.insert(Drug(appl_no=20812,
                        product_no=2,
                        form='SUSPENSION/DROPS;ORAL',
                        strength='50MG/1.25ML',
                        reference_drug=1,
                        drug_name="INFANT'S ADVIL",
                        active_ingredients='IBUPROFEN',
                        reference_standard=1))
    # retrieve 1 records
    infants_advil = drug_db.query('drug_name', "INFANT'S ADVIL").results()
    for drug in infants_advil:
        # print DELETE successfully
        drug_db.delete(drug)
    # retrieve 1 records
    drug_db.query("appl_no", 20812).results()
    # print INSERT successfully
    drug_db.insert(Drug(appl_no=20812,
                        product_no=2,
                        form='SUSPENSION/DROPS;ORAL',
                        strength='50MG/1.25ML',
                        reference_drug=1,
                        drug_name="INFANT'S ADVIL",
                        active_ingredients='IBUPROFEN',
                        reference_standard=1))
    # retrieve 2 records
    drug_db.query("appl_no", 20812).results()
    # retrieve 1 records
    drug_db.query("appl_no", 20812).filter("drug_name", "INFANT'S ADVIL").results()
    # retrieve 1 records
    ferrlecit = drug_db.query('appl_no', 20955).results()
    for drug in ferrlecit:
        # print DELETE successfully
        drug_db.delete(drug)
    # retrieve 0 records
    ferrlecit = drug_db.query('appl_no', 20955).results()
    test_drug = Drug(1111, 1, "form", "strength", 1, "test drug", "active_ingredients", 1)
    # print DELETE ERROR
    drug_db.delete(test_drug)
