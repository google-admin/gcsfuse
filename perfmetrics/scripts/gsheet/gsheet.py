from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1kvHv1OBCzr9GnFxRu9RTJC7jjQjc9M4rAiDnhyak2Sg'

CREDENTIALS_PATH = ('./gsheet/creds.json')

def _get_sheets_service_client():
  creds = service_account.Credentials.from_service_account_file(
      CREDENTIALS_PATH, scopes=SCOPES)
  service = build('sheets', 'v4', credentials=creds)
  return service

def write_to_google_sheet(worksheet: str, data) -> None:
  """Calls the API to update the values of a sheet.

  Args:
    worksheet: string, name of the worksheet to be edited appended by a "!"
    data: list of tuples/lists, data to be added to the worksheet

  Raises:
    HttpError: For any Google Sheets API call related errors
  """
  sheets_client = _get_sheets_service_client()
  spreadsheet_response = sheets_client.spreadsheets().values().get(
      spreadsheetId=SPREADSHEET_ID,
      range='{}!A2:A'.format(worksheet)).execute()
  entries = len(spreadsheet_response['values'])

  sheets_client.spreadsheets().values().update(
      spreadsheetId=SPREADSHEET_ID,
      valueInputOption='USER_ENTERED',
      body={
          'majorDimension': 'ROWS',
          'values': data
      },
      range='{}!A{}'.format(worksheet, entries+2)).execute()
