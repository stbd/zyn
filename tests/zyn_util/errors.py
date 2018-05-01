NoError = 0
ErrorMalformedMessage = 1
InternalCommunicationError = 2
ErrorFileIsNotOpen = 3
ErrorFileOpenedInReadMode = 4
InvalidUsernamePassword = 100
ParentIsNotFolder = 101
UnauthorizedOperation = 102
InternalCommunicationError = 103
InternalError = 104
UnknownFile = 105
UnknownAuthority = 106
AuthorityError = 107
InvalidNodeId = 200
FolderIsNotEmpty = 201
InvalidPathSize = 202
InvalidPath = 203
HostFilesystemError = 204
AllNodesInUse = 205
ParentIsNotFolder = 206
NodeIsNotFile = 207
NodeIsNotFolder = 208
InternalCommunicationError = 300
InternalError = 301
RevisionTooOld = 302
OffsetAndSizeDoNotMapToPartOfFile = 303
DeleteIsonlyAllowedForLastPart = 304
FileLockedByOtherUser = 305
FileNotLocked = 306
InvalidOffsets = 307


def error_to_string(error):
    if error == 0:
        return "NoError"
    elif error == 1:
        return "ErrorMalformedMessage"
    elif error == 2:
        return "InternalCommunicationError"
    elif error == 3:
        return "ErrorFileIsNotOpen"
    elif error == 4:
        return "ErrorFileOpenedInReadMode"
    elif error == 100:
        return "InvalidUsernamePassword"
    elif error == 101:
        return "ParentIsNotFolder"
    elif error == 102:
        return "UnauthorizedOperation"
    elif error == 103:
        return "InternalCommunicationError"
    elif error == 104:
        return "InternalError"
    elif error == 105:
        return "UnknownFile"
    elif error == 106:
        return "UnknownAuthority"
    elif error == 107:
        return "AuthorityError"
    elif error == 200:
        return "InvalidNodeId"
    elif error == 201:
        return "FolderIsNotEmpty"
    elif error == 202:
        return "InvalidPathSize"
    elif error == 203:
        return "InvalidPath"
    elif error == 204:
        return "HostFilesystemError"
    elif error == 205:
        return "AllNodesInUse"
    elif error == 206:
        return "ParentIsNotFolder"
    elif error == 207:
        return "NodeIsNotFile"
    elif error == 208:
        return "NodeIsNotFolder"
    elif error == 300:
        return "InternalCommunicationError"
    elif error == 301:
        return "InternalError"
    elif error == 302:
        return "RevisionTooOld"
    elif error == 303:
        return "OffsetAndSizeDoNotMapToPartOfFile"
    elif error == 304:
        return "DeleteIsonlyAllowedForLastPart"
    elif error == 305:
        return "FileLockedByOtherUser"
    elif error == 306:
        return "FileNotLocked"
    elif error == 307:
        return "InvalidOffsets"
    else:
        raise RuntimeError("Unknown error")
