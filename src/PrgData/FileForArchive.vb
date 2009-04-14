Public Class FileForArchive
    Public FileName As String
    Public FileType As Boolean
    Public Sub New(ByVal _FileName As String, ByVal _FileType As Boolean)
        FileName = _FileName
        FileType = _FileType
    End Sub
End Class
