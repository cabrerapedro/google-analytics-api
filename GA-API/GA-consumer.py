"""Hello Analytics Reporting API V4."""
import argparse
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from utils import _google_api_make_credentials_file_if_not_exist
import utils
import csv
scope = 'https://www.googleapis.com/auth/analytics.readonly'
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
CLIENT_SECRETS_PATH = 'client_secrets.json' # Path to client_secrets.json file.
VIEW_ID = '66889609' #66889609
def _build_google_analytics_service():
    credential_file = 'analyticsreporting.dat'
    scope = 'https://www.googleapis.com/auth/analytics.readonly'
    creds_file = _google_api_make_credentials_file_if_not_exist('google_analytics_token', credential_file)
    store = file.Storage(creds_file)
    creds = store.get()
    if not creds or creds.invalid:
        print('credentials invalid, creating new file')
        flow = client.flow_from_clientsecrets('client_secret.json', scope)
        creds = tools.run_flow(flow, store)
    http = creds.authorize(http=httplib2.Http())
    service = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)
    return service
def _get_response(analytics, query, start_index=0):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    report_parsed = []
    request = {
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          "dimensions": [{"name": "ga:{0}".format(dimension)} for dimension in query['dimensions']],
          'dateRanges': [{'startDate': '15daysAgo', 'endDate': 'today'}],
          'metrics': [{"expression": "ga:{0}".format(dimension)} for dimension in query['metrics']],
          "pageSize": "1000",
          "pageToken": str(start_index)
        }]
      }
    print (request)
    response = analytics.reports().batchGet(body=request).execute()
    report = response.get('reports')[0]
    next_start_index = report.get('nextPageToken')
    data = report.get('data').get('rows')
    for row in data:
        dims = [dimension for dimension in row['dimensions']]
        metrics = [dimension for dimension in row['metrics'][0].get('values')]
        row = dims + metrics
        report_parsed.append(row)
    return [report_parsed , next_start_index]
def get_report(query={'dimensions': ['date'], 'metrics': ['impressions', 'adClicks']}):
    #pagination
    service = _build_google_analytics_service()
    start_index = 0
    report = []
    while start_index is not None:
        response = _get_response(service, query, start_index=start_index)
        report = report + response[0]
        start_index = response[1]
    return report
def write_report_csv(report, filename='ga_report.csv'):
    with open(filename, "w") as file:
        writer = csv.writer(file)
        writer.writerows(report)
        return filename
def run_report():
    #adwords performance
    query_performance = {'dimensions': ['date', 'campaign', 'adwordsCampaignID', 'adGroup', 'adwordsAdGroupID', 'keyword',
                            'adwordsCreativeID'], 'metrics': ['impressions', 'adClicks', 'adCost', 'transactions']}
    report_performance = get_report(query_performance)
    report_file_performance = write_report_csv(report_performance, filename='report_performance.csv')
    utils.merge_increment_from_file('adwords_performance', report_file_performance, merge_columns=['date'], header='', airflow_connection_id='dwh',
                              drop_increment_table_on_finish=True)
    #transactions
    query_transaction = {
        'dimensions': ['date', 'transactionId', 'source', 'medium', 'channelGrouping', 'adwordsCampaignID',
                       'adGroup', 'adwordsAdGroupID', 'keyword'], 'metrics': ['transactions']}
    report_transaction = get_report(query_transaction)
    report_file_trans = write_report_csv(report_transaction, filename='report_transaction.csv')
    utils.merge_increment_from_file('marketing_ga_transactions', report_file_trans, merge_columns=['transactionId'], header='', airflow_connection_id='dwh',
                              drop_increment_table_on_finish=True)
    #de duplicate transactions
    dedup_sql = """drop table if exists dwh.fact_ga_transactions_dedup_new;
                create table dwh.fact_ga_transactions_dedup_new as 
                with numbered_transactions as 
                    (select *, 
                    row_number() over (partition by transactionid order by date asc) as rownum 
                    from dwh.marketing_ga_transactions),
                campaign_name as 
                    (select adwordscampaignid, max(campaign) as campaign
					from dwh.adwords_performance 
					group by 1)
                select n.*, c.campaign
                from numbered_transactions n 
                inner join campaign_name c on c.adwordscampaignid=n.adwordscampaignid
                where rownum = 1;
                drop table if exists dwh.fact_ga_transactions_dedup;
                ALTER table dwh.fact_ga_transactions_dedup_new rename to fact_ga_transactions_dedup;
                    """
    utils.query_pg('dwh', dedup_sql)
if __name__ == '__main__':
    run_report()