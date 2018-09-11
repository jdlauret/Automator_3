import os
# Imports the Google Cloud client library
from google.cloud.client import Client
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build
credentials = GoogleCredentials.get_application_default()
service = build('language', 'v1', credentials=credentials)

# Globals
SCOPES = 'https://www.googleapis.com/auth/cloud-platform'
CLIENT_SECRET_FILE = 'data\\client_secrets.json'
APPLICATION_NAME = 'PythonDriveReader'

nlu = language.LanguageServiceClient(credentials=credentials)

# The text to analyze
text = u'Hello, world!'
document = types.Document(
    content=text,
    type=enums.Document.Type.PLAIN_TEXT)

# Detects the sentiment of the text
sentiment = nlu.analyze_sentiment(document=document).document_sentiment

print('Text: {}'.format(text))
print('Sentiment: {}, {}'.format(sentiment.score, sentiment.magnitude))