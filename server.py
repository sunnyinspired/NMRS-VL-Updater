import mysql.connector as mysql_con
import csv
from dotenv import load_dotenv
import os
import datetime

# load the dotenv
load_dotenv()
host = os.getenv('DB_HOST')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_DATABASE')
# connect to database
con = mysql_con.connect(host=host, user=user, password=password, database=database)

query = con.cursor()

client_facility = 53
counter = 0
# get current date
cur_date = datetime.date.today()
today = cur_date.strftime('%Y-%m-%d')
# open the csv file
with open('data/data.csv') as file:
    csv_file = csv.reader(file)
    # skip the header in the sheet
    next(csv_file)
    # loop through each row in the csv file
    for row in csv_file:
        # get the patient identifier column
        pid = row[7]
        visit_date = row[24]
        vl_result = row[23]
        # print(f"VL result is: {vl_result}")
        # get the patient_id from the database for each patient using the patient identifier
        query.execute(f"SELECT patient_id FROM patient_identifier WHERE identifier = '{pid}'")
        results = query.fetchall()
        # the result is a list of tuples, so we loop through the result
        for r in results:
            try:
                # for each of the retrieved ids, we insert the corresponding data into the relevant tables.
                query.execute("SET FOREIGN_KEY_CHECKS = 0")
                # insert into visit table
                insert1 = "INSERT INTO `visit`(`patient_id`,`visit_type_id`,`date_started`, `date_stopped`," \
                          "`location_id`,`creator`,`date_created`,`uuid`)" \
                          "VALUES('%s', '%s', %s, %s, '%s', '%s', NOW(), UUID())"
                stop_date = visit_date + ' 23:59:59'
                values1 = (r[0], 1, visit_date, stop_date, client_facility, 1)
                query.execute(insert1, values1)
                con.commit()

                # get last visit_id
                query.execute("SELECT MAX(visit_id) FROM visit")
                get_visit = query.fetchone()

                # insert into encounter table
                insert2 = "INSERT INTO `encounter`(`encounter_type`,`patient_id`,`location_id`,`form_id`," \
                          "`encounter_datetime`,`creator`,`date_created`,`visit_id`, `uuid`) " \
                          "VALUES('%s', '%s', '%s', '%s', %s, '%s', NOW(), '%s', UUID())"
                values2 = (11, r[0], client_facility, 21, visit_date, 1, get_visit[0])
                query.execute(insert2, values2)
                con.commit()

                # get last encounter
                query.execute("SELECT MAX(encounter_id) FROM encounter WHERE encounter_type='11'")
                get_encounter = query.fetchone()

                insert3 = "INSERT INTO encounter_provider(`encounter_id`,`provider_id`,`encounter_role_id`,`creator`," \
                          "`date_created`,`uuid`)" \
                          "VALUES('%s', '%s','%s','%s',NOW(), UUID())"
                values3 = (get_encounter[0], 9, 2, 1)
                query.execute(insert3, values3)
                con.commit()

                # insert into OBS table
                insertObs = "INSERT INTO `obs`(`person_id`, `concept_id`, `encounter_id`, `obs_datetime`, " \
                            "`location_id`, `value_coded`, `value_datetime`, `value_numeric`, `value_text`, " \
                            "`creator`, `date_created`,`uuid`)" \
                            "VALUES('%s', '%s', '%s', %s, '%s', %s, %s, %s, %s, '1', NOW(),UUID())"
                valuesObs = [
                    (r[0], 165765, get_encounter[0], visit_date, client_facility, 2, None, None, None),
                    (r[0], 162476, get_encounter[0], visit_date, client_facility, 1000, None, None, None),
                    (r[0], 164980, get_encounter[0], visit_date, client_facility, 161236, None, None, None),
                    (r[0], 159951, get_encounter[0], visit_date, client_facility, None, visit_date, None, None),
                    (r[0], 165988, get_encounter[0], visit_date, client_facility, None, visit_date, None, None),
                    (r[0], 165716, get_encounter[0], visit_date, client_facility, None, visit_date, None, None),
                    (r[0], 165987, get_encounter[0], visit_date, client_facility, None, today, None, None),
                    (r[0], 166423, get_encounter[0], visit_date, client_facility, None, today, None, None),
                    (r[0], 166422, get_encounter[0], visit_date, client_facility, 166426, None, None, None),
                    (r[0], 856, get_encounter[0], visit_date, client_facility, None, None, vl_result, None),
                    (r[0], 164989, get_encounter[0], visit_date, client_facility, None, visit_date, None, None)
                ]
                query.executemany(insertObs, valuesObs)
                con.commit()
                query.execute("SET FOREIGN_KEY_CHECKS = 1")
                counter += 1
            except mysql_con.Error as e:
                print("Failed to Insert Data: ", e)
print(f"Viral Load Results Successfully Updated for {counter} Patients")
