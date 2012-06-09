update
  logs.DocumentSendLogs,
  documents.DocumentHeaders
set
  DocumentDelivered = 1
where
    DocumentSendLogs.Committed = 1
and DocumentHeaders.DownloadId = DocumentSendLogs.DocumentId;