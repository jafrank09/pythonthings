import pandas as pd
import boto3
from datetime import date
import os
import time
import sys
import psycopg2

# Need to get report names changed to be env-specific to really complete this

# globals
currentDate = date.today()
reportList = [f'LedgerReport-{currentDate}.csv', f'TransactionsReport-{currentDate}.csv', f'ActiveRevenueReport-{currentDate}.csv', f'BillingReport-{currentDate}.csv']
s3_client = boto3.client('s3')
date_string = '{:%Y-%m-%d}'.format(currentDate)
fin_report_name = ['generateTransactionsReport', 'generateActiveRevenueReport', 'generateBillingReport']


def createdbconnection():
    connection = psycopg2.connect(host='host', port=1234, database='dbname', user='user', password='password')
    return connection


def verifyreportaskrunner():
    with createdbconnection().cursor() as cur:
        cur.execute("select * from task_runner_toggle")
        results = cur.fetchall()
        for record in results:
            if record[1] == 'Report Task Runner' and record[2] is False:
                cur.execute("UPDATE task_runner_toggle SET is_active = True WHERE task_runner_name = 'Report Task Runner'")
                print("Report Task Runner was Toggled ON")
            elif record[1] == 'Report Task Runner' and record[2] is True:
                print("Report Task Runner was Already ON")
    return True


def verifyreporttasksexist():
    report_task_count= 0
    with createdbconnection().cursor() as cur:
        cur.execute("select * from report_task where method ilike '%generate%' ORDER by id DESC limit 4")
        results = cur.fetchall()
        for record in results:
            if record[2] in fin_report_name and date_string in record[3]:
                print(f"'{record[2]}' task was found")
                report_task_count += 1
            elif record[2] == 'generateLedgerReport':
                print(f"'{record[2]}' task was found")
                report_task_count += 1
    if report_task_count is not 4:
        return sys.exit("REPORT TASKS MISSING")
    else:
        return report_task_count


def verifytaskstatus():
    report_completion_count = 0
    complete_task_list = []
    while report_completion_count < 4:
        with createdbconnection().cursor() as cur:
            cur.execute("select * from report_task where method ilike '%generate%' ORDER by id DESC limit 4")
            results = cur.fetchall()
            for record in results:
                if record[4] == 'finished' and record[2] not in complete_task_list:
                    report_completion_count += 1
                    complete_task_list.append(record[2])
        if report_completion_count == 4:
            print(f"All {report_completion_count} reports Are generated!:  {complete_task_list}")
            #and complete_task_list in fin_report_name:
            return report_completion_count
        elif report_completion_count < 4:
            print(f" {report_completion_count} have finished generating: {complete_task_list}")
            time.sleep(5.5)

def verifyreportins3(reportList):
    for report in reportList:
        reports_in_bucket = s3_client.list_objects(Bucket='bucket', Prefix='prefix')['Contents']
        split_report_name = (reports_in_bucket[0]['Key'].split('/', 1))
        if split_report_name[1] not in reportList:
            print(f"THE {report} FOR TODAY IS NOT IN S3\n")
        else:
            print(f"The {report} for today is in s3\n")
            popup = f'''osascript -e 'display notification "The file  {report}  was found in S3" with title "A finance report was located"'
                        '''
            time.sleep(3)
            os.system(popup)
    return print("ALL REPORTS PRESENT IN S3")


def downloadfinancereport(report_list):
    for report in reportList:
        s3_client.download_file('bucket', 'key', f'/path/')
        popup = f'''osascript -e 'display notification "The file {report} is downloading from S3" with title "Downloading Report"'
                '''
        os.system(popup)
        time.sleep(3)
    return True


def verifyreportdownload(reportnames):
    missingreports = 0
    for report in reportnames:
        if report not in os.listdir("/filepath/"):
            missingreports += 1
            print(f"THE {report} IS MISSING\n")
            popup = f'''osascript -e 'display notification "{report} WAS NOT DOWNLOADED CORRECTLY " with title "REPORT DOWNLOAD ERROR"'
                    '''
            os.system(popup)
            time.sleep(3)
        elif report in os.listdir("/filepath/"):
            print(f"the following report has downloaded successfully: {report} \n")
            popup = f'''osascript -e 'display notification "{report} was downloaded successfully" with title "report successfully downloaded"'
                                '''
            os.system(popup)
            time.sleep(3)
        assert missingreports == 0
    return missingreports



def reportkeys(report):
    readreport = pd.read_csv(report)
    reportkeyslist = readreport.keys().tolist()
    # print(reportkeyslist)
    return reportkeyslist


def ledgersums(ledger):
    if ledger == "control_ledger.csv":
        reportname = "LEDGER CONTROL"
    elif ledger == "test_ledger.csv":
        reportname = "LEDGER TEST   "
    ledgerreport = pd.read_csv(ledger)
    ledgersum = (ledgerreport[['data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column']].sum())
    finalsums = ledgersum.tolist()
    print(reportname, finalsums)
    return finalsums


def billingsums(billing):
    if billing == "control_billing.csv":
        reportname = "BILLING CONTROL"
    elif billing == "test_billing.csv":
        reportname = "BILLING TEST   "
    billingreport = pd.read_csv(billing)
    billingsum = (billingreport[['data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column']].sum())
    finalsums = billingsum.tolist()
    print(reportname, finalsums)
    return finalsums


def activerevenuesums(activerevenue):
    if activerevenue == "control_active_revenue.csv":
        reportname = "ACTIVE REVENUE CONTROL"
    elif activerevenue == "test_active_revenue.csv":
        reportname = "ACTIVE REVENUE TEST   "
    activerevenuereport = pd.read_csv(activerevenue)
    activerevsum = (activerevenuereport[
                       ['data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column']].sum())
    finalsums = activerevsum.tolist()
    print(reportname, finalsums)
    return finalsums


def transactionssums(transactions):
    if transactions == "control_transactions.csv":
        reportname = "TRANSACTIONS CONTROL"
    elif transactions == "test_transactions.csv":
        reportname = "TRANSACTIONS TEST   "
    transactionsreport = pd.read_csv(transactions)
    transactionssum = (transactionsreport[['data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column', 'data_Column']].sum())
    finalsums = transactionssum.tolist()
    print(reportname, finalsums)
    return finalsums


def reportkeycomparison(test, control):
    if test == control:
        print("\n REPORT KEYS MATCH\n")
    else:
        print("\n REPORT KEYS DO NOT MATCH!!!\n")


def sumcomparison(test, control):
    if test == control:
        print("\n ^^^ REPORT TOTALS MATCH\n -----")
    else:
        print("\n ^^^ REPORT TOTALS DOES NOT MATCH!!!!\n -----")


if __name__ == '__main__':
    #---- NOTE: YOU WILL NEED FULL ROOT ACCESS TO DB'S TO AUTH IN, UNTIL THATS RESOLVED, CALLING THE FIRST THREE METHODS ISNT POSSIBLE, THE REST CAN STILL BE RUN AND USED ----#

    # # Verify report_task_runner_toggle is ON in DB (if not, this turns it on)
    # verifyreportaskrunner()
    #
    # # Verify that Today's report tasks were created in the report_task table
    # verifyreporttasksexist()
    #
    # # Check continously until 4 most recent report tasks (qrtz trigger creates these automatically), have a status of finished
    # if verifytaskstatus() != 4:
    #     sys.exit("SOME REPORT TASKS DID NOT COMPLETE")
    # else:


        # Access the s3 bucket, verify that the finance reports for today are present
        print("Verifying today's reports created in S3\n")
        verifyreportins3(reportList)

        # pull down the 4 finance reports with the current date from list
        print("Downloading Finance Reports\n")
        downloadfinancereport(reportList)

        # Check that reports are present in local project folder and properly named
        if verifyreportdownload(reportList) > 0:
            sys.exit("ALL REPORTS MUST BE DOWNLOADED BEFORE VALIDATION CAN BEGIN")

        else:

            # ledger report comparisons
            print("RUNNING LEDGER REPORT COMPARISON\n")
            reportkeycomparison(reportkeys("control_ledger.csv"), (reportkeys("test_ledger.csv")))
            sumcomparison(ledgersums("control_ledger.csv"), ledgersums("test_ledger.csv"))

            # billing report comparisons
            print("RUNNING BILLING REPORT COMPARISON\n")
            reportkeycomparison(reportkeys("control_billing.csv"), (reportkeys("test_billing.csv")))
            sumcomparison(billingsums("control_billing.csv"), billingsums("test_billing.csv"))

            # active revenue report comparisons
            print("RUNNING ACTIVE REVENUE REPORT COMPARISON\n")
            reportkeycomparison(reportkeys("control_active_revenue.csv"), (reportkeys("test_active_revenue.csv")))
            sumcomparison(activerevenuesums("control_active_revenue.csv"), activerevenuesums("test_active_revenue.csv"))

            # transactions report comparisons
            print("RUNNING TRANSACTIONS REPORT COMPARISON\n")
            reportkeycomparison(reportkeys("control_transactions.csv"), (reportkeys("test_transactions.csv")))
            sumcomparison(transactionssums("control_transactions.csv"), transactionssums("test_transactions.csv"))
